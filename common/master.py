import json
import multiprocessing
import signal

from common.duplicate_handler import DuplicateHandler
from common.logger import get_logger
from common.mom import RabbitMQProcessor

logger = get_logger("MasterLogic")

EOS_TYPE = "EOS"
REC_TYPE = "RECOVERY"

class MasterLogic(multiprocessing.Process):
    def __init__(self, config, manager, node_id, nodes_of_type, clean_queues, 
                 client_manager, started_event=None,
                 movies_handler=None, sharded=False, shard_mapping=None):
        """
        Initialize the MasterLogic class with the given configuration and manager.
        """
        super().__init__()
        self.stopped = False
        if not isinstance(clean_queues, list):
            self.clean_queues = [clean_queues]
        else:
            self.clean_queues = clean_queues
        self.config = config
        self.rabbitmq_processor = RabbitMQProcessor(
            config,
            source_queues=self.clean_queues,
            target_queues=[clean_queue + "_node_" + str(i) for i in range(1, nodes_of_type + 1) for clean_queue in self.clean_queues],
        )
        self.manager = manager
        self.node_id = node_id
        self.nodes_of_type = nodes_of_type
        self.target_index = 1
        self.leader = multiprocessing.Event()
        self.client_manager = client_manager
        self.movies_handler = movies_handler
        self.started_event = started_event
        
        # Sharding configuration
        self.sharded = sharded
        self.shard_mapping = shard_mapping or {}  # {node_name: (start_range, end_range)}

        self.duplicate_handler = DuplicateHandler()

        if not self.rabbitmq_processor.connect():
            logger.error("Error connecting to RabbitMQ. Exiting...")
            return
        
        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)
        signal.signal(signal.SIGINT, self.__handleSigterm)

    def __handleSigterm(self, signum, frame):
        logger.info("SIGTERM signal received. Closing connection...")
        try:
            self._close_connection()
        except Exception as e:
            logger.info(f"Error closing connection: {e}")

    def _get_node_for_shard_id(self, shard_id, queue_name):
        """
        Find which node should handle a given shard ID.
        Returns the target node queue name.
        """
        if not self.sharded or not self.shard_mapping:
            # Fallback to round-robin if not sharded
            return f"{queue_name}_node_{((shard_id % self.nodes_of_type) + 1)}"
        
        # Find the node that handles this shard range
        for node_name, (start_range, end_range) in self.shard_mapping.items():
            if start_range <= shard_id <= end_range:
                # Extract node number from node_name (e.g., "join_credits_2" -> "2")
                node_num = node_name.split('_')[-1]
                return f"{queue_name}_node_{node_num}"
        
        # Fallback if no shard range found
        logger.warning(f"No shard range found for shard_id {shard_id}, using round-robin")
        return f"{queue_name}_node_{((shard_id % self.nodes_of_type) + 1)}"


    def load_balance(self, ch, method, properties, body, queue_name):
        """
        Callback function to handle load balancing of messages.
        It will distribute the messages to the appropriate queues.
        """
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            headers = getattr(properties, "headers", {}) or {}
            client_id = headers.get("client_id", None)
            message_id = headers.get("message_id", None)
            
            try:
                decoded = json.loads(body)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode message body: {body}")
                return
                
            if msg_type == EOS_TYPE:
                # Publish the EOS message to all nodes
                for i in range(1, self.nodes_of_type + 1):
                    self.rabbitmq_processor.publish(
                        target=f"{queue_name}_node_{i}",
                        message=decoded,
                        msg_type=msg_type,
                        headers=properties.headers,
                    )
            elif msg_type == REC_TYPE:
                self._handle_node_recovery(decoded, queue_name)
                # if self.movies_handler is not None:
                #     self.movies_handler.recover_movies_table(self.node_id)
            else:
                if client_id is None:
                    logger.error("Missing client_id in headers")
                    return
                if message_id is None:
                    logger.error("Missing message_id in headers")
                    return
                if self.duplicate_handler.is_duplicate(client_id, queue_name, message_id):
                    logger.info("Duplicate message detected: %s. Acknowledging without processing.", message_id)
                    return
                
                # Determine target node based on sharding or round-robin

                if self.sharded:
                    logger.debug("Sharded mode enabled. Decoding shard_id from message: %s", decoded)
                    messages = decoded if isinstance(decoded, list) else [decoded]

                    node_batches = {}  # target_node -> list of messages

                    for i, message in enumerate(messages):
                        if not isinstance(message, dict):
                            logger.error("Invalid message type: %s. Expected dict. Skipping.", type(message))
                            continue

                        shard_id = message.get("id") or message.get("movie_id")
                        if shard_id is None:
                            logger.error("Missing shard_id (id or movie_id) in message %s", message)
                            continue

                        target_node = self._get_node_for_shard_id(shard_id, queue_name)
                        logger.debug("Target node for shard_id %s is %s", shard_id, target_node)

                        if target_node not in node_batches:
                            node_batches[target_node] = []
                        node_batches[target_node].append(message)

                    expected_parts = len(node_batches)
                    for sub_id, (target_node, batch) in enumerate(node_batches.items()):
                        self.target_index = int(target_node.split('_')[-1])  # Adjust according to your naming
                        logger.info(
                            "Distributing batch of %d messages to node %d (node: %s) for queue %s",
                            len(batch), self.target_index, target_node, queue_name
                        )

                        headers["sub_id"] = sub_id
                        headers["expected"] = expected_parts
                        self.rabbitmq_processor.publish(
                            target=target_node,
                            message=batch,  # send list of messages
                            msg_type=msg_type,
                            headers=headers,
                        )


                else:
                    # Original round-robin logic
                    self.target_index = (message_id % self.nodes_of_type) + 1
                    logger.debug(f"Distributing message {message_id} from client {client_id} to node {self.target_index} for queue {queue_name}")
                    target_node = f"{queue_name}_node_{self.target_index}"
                    self.rabbitmq_processor.publish(
                        target=target_node,
                        message=decoded,
                        msg_type=msg_type,
                        headers=headers,
                    )
                if message_id:
                    self.duplicate_handler.add(client_id, queue_name, message_id)
        except Exception as e:
            logger.error(f"Error in load_balance callback: {e}")
        finally:
            self.rabbitmq_processor.acknowledge(method)
            if self.leader.is_set():
                logger.debug(f"Message processed and acknowledged for node {self.target_index}")
            else:
                logger.info("Leader untoggled, stopping consumption.")
                self.rabbitmq_processor.stop_consuming()

    def run(self):
        """
        Run the master logic process.
        """                
        try:
            if self.started_event:
                self.started_event.set()
            while not self.stopped:
                self.leader.wait() # Wait to be set as leader
                self.rabbitmq_processor.consume(self.load_balance) # Consume messages from the queue
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
        finally:
            self._close_connection()
        
    def _handle_node_recovery(self, data, queue_name):
        """
        Handle the recovery process for a node, sending all current EOS messages.
        This method will be called when a node requests recovery.
        """
        node_id = data.get("node_id")
        if not node_id:
            logger.error("Node ID not found in recovery request.")
            return
        
        logger.info(f"Node {node_id} requested recovery for queue {queue_name}.")
        clients = self.client_manager.get_clients()
        logger.info(f"Clients for recovery: {clients}")
        if not clients:
            logger.warning("No clients found for recovery.")
            return
        logger.info(f"Clients found: {clients.items()}")
        # Send all EOS messages to the requesting node for the specified queue
        for client_id, eos_flags in clients.items():
            if queue_name in eos_flags.keys():
                for target_node in eos_flags[queue_name]:
                    self.rabbitmq_processor.publish(
                        target=f"{queue_name}_node_{node_id}",
                        message={"node_id": target_node},
                        msg_type=EOS_TYPE,
                        headers={"client_id": client_id},
                        priority=1                           
                    )
        
        # Acknowledge the recovery request
        self.rabbitmq_processor.publish(
            target=f"{queue_name}_node_{node_id}",
            message={"status": "recovery_complete"},
            msg_type=REC_TYPE,
            headers={"node_id": node_id, "queue_name": queue_name},
            priority=1
        )
        logger.info(f"EOS messages sent to node {node_id} for queue {queue_name}.")

    def _close_connection(self):
        if not self.stopped:
            try:
                logger.info("Closing RabbitMQ connection...")
                self.rabbitmq_processor.stop_consuming()
                self.rabbitmq_processor.close()
                logger.info("Connection closed.")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self.stopped = True

    def toggle_leader(self):
        """
        Toggle the leader status.
        """
        if not self.leader.is_set():
            self.leader.set()
        else:
            self.leader.clear()

        logger.info(f"Leader status toggled to: {self.leader.is_set()}")

    def is_leader(self):
        """
        Check if the current node is the leader.
        """
        return self.leader.is_set()
