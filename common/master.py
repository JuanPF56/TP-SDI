import json
import multiprocessing
import signal

from common.client_state_manager import ClientManager
from common.client_state import ClientState
from common.duplicate_handler import DuplicateHandler
from common.logger import get_logger
from common.mom import RabbitMQProcessor

logger = get_logger("MasterLogic")

EOS_TYPE = "EOS"
REC_TYPE = "RECOVERY"

class MasterLogic(multiprocessing.Process):
    def __init__(self, config, manager, node_id, nodes_of_type, clean_queues, 
                 client_manager,
                 started_event=None,
                 movies_handler=None):
        """
        Initialize the MasterLogic class with the given configuration and manager.
        """
        super().__init__(target=self.run)
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
                if self.movies_handler is not None:
                    self.movies_handler.recover_movies_table(self.node_id)
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
        if not clients:
            logger.warning("No clients found for recovery.")
            return
        # Send all EOS messages to the requesting node for the specified queue
        for client_id, client_state in clients.items():
            if client_state:
                eos_flags = client_state.get_eos_flags()
                if queue_name in eos_flags:
                    for target_node in eos_flags[queue_name]:
                        self.rabbitmq_processor.publish(
                            target=f"{queue_name}_node_{node_id}",
                            message={"node_id": target_node},
                            msg_type=EOS_TYPE,
                            headers={"client_id": client_id},
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
        self.leader.set() if not self.leader.is_set() else self.leader.clear()
        logger.info(f"Leader status toggled to: {self.leader.is_set()}")

    def is_leader(self):
        """
        Check if the current node is the leader.
        """
        return self.leader.is_set()
