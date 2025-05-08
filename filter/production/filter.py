import configparser
import json

from common.eos_handling import mark_eos_received
from common.logger import get_logger
logger = get_logger("Filter-Production")
from collections import defaultdict
from common.filter_base import FilterBase, EOS_TYPE
from common.client_state_manager import ClientManager
from common.client_state import ClientState

class ProductionFilter(FilterBase):
    def __init__(self, config):
        """
        Initialize the ProductionFilter with the provided configuration.
        """
        super().__init__(config)
        self.batch_arg = defaultdict(list)
        self.batch_solo = defaultdict(list)
        self.batch_arg_spain = defaultdict(list)
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

    def _publish_batch(self, queue, batch, headers):
        self.rabbitmq_processor.publish(target=queue, message=batch, headers=headers)
        logger.debug(f"Sent batch to {queue}")

    def _handle_eos(self, queue_name, body, method, headers, client_state: ClientState):
        if client_state:
            key = (client_state.client_id, client_state.request_id)
            logger.debug(f"Received EOS from {queue_name}")
            if len(self.batch_arg[key]) > 0:
                self._publish_batch(queue=self.target_queues[queue_name][0], batch=self.batch_arg[key], headers=headers)
                self.batch_arg[key] = []
            if len(self.batch_solo[key]) > 0:
                self._publish_batch(queue=self.target_queues[queue_name][1], batch=self.batch_solo[key], headers=headers)
                self.batch_solo[key] = []
            if len(self.batch_arg_spain[key]) > 0:
                self._publish_batch(queue=self.target_queues[queue_name][2], batch=self.batch_arg_spain[key], headers=headers)
                self.batch_arg_spain[key] = []
        mark_eos_received(body, self.node_id, queue_name, self.source_queues, headers, self.nodes_of_type,
                          self.rabbitmq_processor, client_state, self.client_manager,
                          target_queues=self.target_queues.get(queue_name))
        self.rabbitmq_processor.acknowledge(method)

    def _process_movies_batch(self, movies_batch, client_state):
        key = (client_state.client_id, client_state.request_id)
        for movie in movies_batch:
            country_dicts = movie.get("production_countries", [])
            country_names = [c.get("name") for c in country_dicts if "name" in c]

            logger.debug(f"Production countries: {country_names}")

            if "Argentina" in country_names:
                self.batch_arg[key].append(movie)

            if len(country_names) == 1:
                self.batch_solo[key].append(movie)

            if "Argentina" in country_names and "Spain" in country_names:
                self.batch_arg_spain[key].append(movie)

    def _publish_ready_batches(self, queue_name, headers, client_state: ClientState):
        key = (client_state.client_id, client_state.request_id)
        if len(self.batch_arg[key]) >= self.batch_size:
            self._publish_batch(queue=self.target_queues[queue_name][0], batch=self.batch_arg[key], headers=headers)
            self.batch_arg[key] = []

        if len(self.batch_solo[key]) >= self.batch_size:
            self._publish_batch(queue=self.target_queues[queue_name][1], batch=self.batch_solo[key], headers=headers)
            self.batch_solo[key] = []

        if len(self.batch_arg_spain[key]) >= self.batch_size:
            self._publish_batch(queue=self.target_queues[queue_name][2], batch=self.batch_arg_spain[key], headers=headers)
            self.batch_arg_spain[key] = []

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to process batched messages from the input queue.
        Filters movies by production countries and sends them in batches to the appropriate queues.
        """
        msg_type = self._get_message_type(properties)
        headers = getattr(properties, "headers", {}) or {}
        client_id, request_number = headers.get("client_id"), headers.get("request_number")

        if not client_id or not request_number:
            logger.error("Missing client_id or request_number in headers")
            self.rabbitmq_processor.acknowledge(method)
            return
    
        client_state = self.client_manager.add_client(client_id, request_number, msg_type==EOS_TYPE)

        if msg_type == EOS_TYPE:
            self._handle_eos(queue_name, body, method, headers, client_state)
            return

        try:
            movies_batch = self._decode_body(body, queue_name)
            if not movies_batch:
                self.rabbitmq_processor.acknowledge(method)
                return

            self._process_movies_batch(movies_batch, client_state)
            self._publish_ready_batches(queue_name, headers, client_state)

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
