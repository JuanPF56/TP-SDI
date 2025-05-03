import configparser
import json

from common.logger import get_logger
logger = get_logger("Filter-Production")

from common.filter_base import FilterBase, EOS_TYPE
from common.client_state_manager import ClientManager
from common.client_state import ClientState
class ProductionFilter(FilterBase):
    def __init__(self, config):
        """
        Initialize the ProductionFilter with the provided configuration.
        """
        super().__init__(config)
        self.batch_arg = []
        self.batch_solo = []
        self.batch_arg_spain = []
        self._initialize_queues()
        self._initialize_rabbitmq_processor()
        self.client_manager = ClientManager(
            expected_queues=self.source_queues,
            nodes_to_await=self.eos_to_await,
        )

    def _initialize_queues(self):
        defaults = self.config["DEFAULT"]
        self.source_queues = [defaults.get("movies_clean_queue", "movies_clean")]
        self.target_queues = {
            self.source_queues[0]: [
                defaults.get("movies_argentina_queue", "movies_argentina"),
                defaults.get("movies_solo_queue", "movies_solo"),
                defaults.get("movies_arg_spain_queue", "movies_arg_spain")
            ]
        }
    
    def setup(self):
        self._initialize_queues()
        self._initialize_rabbitmq_processor()

    def _mark_eos_received(self, body, input_queue, headers, client_state: ClientState):
        """
        Mark the end of stream (EOS) for the given node
        """
        try:
            data = json.loads(body)
            node_id = data.get("node_id")
            count = data.get("count", 0)
        except json.JSONDecodeError:
            logger.error("Failed to decode EOS message")
            return
            
        if not client_state.has_queue_received_eos_from_node(input_queue, node_id):
            count +=1
        
        # If this isn't the last node, send the EOS message back to the input queue
        if count < self.nodes_of_type:
            # Send EOS back to input queue for other production nodes
            self.rabbitmq_processor.publish(
                target=input_queue,
                message={"node_id": node_id, "count": count},
                msg_type=EOS_TYPE,
                headers=headers
            )
        

        client_state.mark_eos(input_queue, node_id)

    def _send_eos(self, headers, client_state: ClientState):
        """
        Propagate the end of stream (EOS) to all output queues if all nodes have sent EOS.
        """
        logger.info("Checking if all nodes have sent EOS")
        if client_state.has_received_all_eos(self.source_queues):
            logger.info("All nodes have sent EOS. Sending EOS to output queues.")
            for queue_list in self.target_queues.values():
                for queue in queue_list:
                    self.rabbitmq_processor.publish(
                        target=queue,
                        message={"node_id": self.node_id, "count": 0},
                        msg_type=EOS_TYPE,
                        headers=headers
                    )
                    logger.debug(f"EOS message sent to {queue}")
            self.client_manager.remove_client(self.current_client_state)

    def _publish_batch(self, queue, batch, headers):
        self.rabbitmq_processor.publish(target=queue, message=batch, headers=headers)
        logger.debug(f"Sent batch to {queue}")

    def _handle_eos(self, queue_name, body, method, headers, client_state):
        logger.debug(f"Received EOS from {queue_name}")

        if len(self.batch_arg) > 0:
            self._publish_batch(queue=self.target_queues[queue_name][0], batch=self.batch_arg, headers=headers)
            self.batch_arg = []

        if len(self.batch_solo) > 0:
            self._publish_batch(queue=self.target_queues[queue_name][1], batch=self.batch_solo, headers=headers)
            self.batch_solo = []

        if len(self.batch_arg_spain) > 0:
            self._publish_batch(queue=self.target_queues[queue_name][2], batch=self.batch_arg_spain, headers=headers)
            self.batch_arg_spain = []

        self._mark_eos_received(body, queue_name, headers, client_state)
        self._send_eos(headers, client_state)
        self.rabbitmq_processor.acknowledge(method)

    def _process_movies_batch(self, movies_batch):
        for movie in movies_batch:
            country_dicts = movie.get("production_countries", [])
            country_names = [c.get("name") for c in country_dicts if "name" in c]

            logger.debug(f"Production countries: {country_names}")

            if "Argentina" in country_names:
                self.batch_arg.append(movie)

            if len(country_names) == 1:
                self.batch_solo.append(movie)

            if "Argentina" in country_names and "Spain" in country_names:
                self.batch_arg_spain.append(movie)

    def _publish_ready_batches(self, queue_name, headers):
        if len(self.batch_arg) >= self.batch_size:
            self._publish_batch(queue=self.target_queues[queue_name][0], batch=self.batch_arg, headers=headers)
            self.batch_arg = []

        if len(self.batch_solo) >= self.batch_size:
            self._publish_batch(queue=self.target_queues[queue_name][1], batch=self.batch_solo, headers=headers)
            self.batch_solo = []

        if len(self.batch_arg_spain) >= self.batch_size:
            self._publish_batch(queue=self.target_queues[queue_name][2], batch=self.batch_arg_spain, headers=headers)
            self.batch_arg_spain = []

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to process batched messages from the input queue.
        Filters movies by production countries and sends them in batches to the appropriate queues.
        """
        msg_type = self._get_message_type(properties)
        headers = getattr(properties, "headers", {}) or {}
        self.current_client_id = headers.get("client_id")
        self.current_request_number = headers.get("request_number")

        if not self.current_client_id or not self.current_request_number:
            logger.error("Missing client_id or request_number in headers")
            self.rabbitmq_processor.acknowledge(method)
            return
    
        client_state = self.client_manager.add_client(self.current_client_id, self.current_request_number)

        if msg_type == EOS_TYPE:
            self._handle_eos(queue_name, body, method, headers, client_state)
            return

        try:
            movies_batch = self._decode_body(body, queue_name)
            if not movies_batch:
                self.rabbitmq_processor.acknowledge(method)
                return

            self._process_movies_batch(movies_batch)
            self._publish_ready_batches(queue_name, headers)

        except Exception as e:
            logger.error(f"Error processing message from {queue_name}: {e}")

        finally:
            self.rabbitmq_processor.acknowledge(method)

    def process(self):
        """
        Main processing function for the ProductionFilter.

        It filters movies based on their production countries and sends them to the respective queues:
        - movies_argentina: for movies produced in Argentina
        - movies_solo: for movies produced in only one country
        - movies_arg_spain: for movies produced in both Argentina and Spain
        """
        logger.info("ProductionFilter is starting up")
        self.run_consumer()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    production_filter = ProductionFilter(config)
    production_filter.setup()
    production_filter.process()
