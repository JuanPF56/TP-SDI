import json
import multiprocessing
import signal

from common.logger import get_logger
from common.mom import RabbitMQProcessor

logger = get_logger("MasterLogic")

EOS_TYPE = "EOS"

class MasterLogic(multiprocessing.Process):
    def __init__(self, config, manager, node_id, nodes_of_type, clean_queues):
        """
        Initialize the MasterLogic class with the given configuration and manager.
        """
        super().__init__(target=self.run)
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
        self.current_node_id = 1
        self.nodes_of_type = nodes_of_type
        self.leader = multiprocessing.Event()
        self.stopped = False

        self.rabbitmq_processor.start()

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)
        signal.signal(signal.SIGINT, self.__handleSigterm)

    def __handleSigterm(self, signum, frame):
        self.log_info("SIGTERM signal received. Closing connection...")
        try:
            self._close_connection()
        except Exception as e:
            self.log_info(f"Error closing connection: {e}")

    def run(self):
        """
        Run the master logic process.
        """
        def load_balance(self, ch, method, properties, body, queue_name):
            """
            Callback function to handle load balancing of messages.
            It will distribute the messages to the appropriate queues.
            """
            try:
                msg_type = properties.type if properties and properties.type else "UNKNOWN"
                if msg_type == EOS_TYPE:
                    # Publish the EOS message to all nodes
                    for i in range(1, self.nodes_of_type + 1):
                        self.rabbitmq_processor.publish(
                            target=f"{self.clean_queue}_node_{i}",
                            message=body,
                            msg_type=msg_type,
                            headers=properties.headers,
                        )
                else:
                    # Round-robin distribution of messages to nodes
                    # TODO: Hash the message to determine the target node
                    target_node = f"{queue_name}_node_{self.current_node_id}"
                    self.rabbitmq_processor.publish(
                        target=target_node,
                        message=body,
                        msg_type=msg_type,
                        headers=properties.headers,
                    )
                    # Increment the current node ID for the next message
                    self.current_node_id = (self.current_node_id % self.nodes_of_type) + 1
            except Exception as e:
                logger.error(f"Error in load_balance callback: {e}")
            finally:
                self.rabbitmq_processor.acknowledge(method)
                if self.leader.is_set():
                    logger.debug(f"Message processed and acknowledged for node {self.current_node_id}")
                else:
                    logger.info("Leader untoggled, stopping consumption.")
                    self.rabbitmq_processor.stop_consuming()
                
        try:
            while not self.stopped:
                self.leader.wait() # Wait to be set as leader
                self.rabbitmq_processor.consume(load_balance) # Consume messages from the queue
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
        finally:
            self._close_connection()
        
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
