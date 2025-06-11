import configparser
from collections import defaultdict

from common.filter_base import FilterBase, EOS_TYPE
from common.client_state_manager import ClientManager
from common.client_state import ClientState
from common.eos_handling import handle_eos
from common.logger import get_logger

logger = get_logger("Filter-Production")


class ProductionFilter(FilterBase):
    def __init__(self, config):
        """
        Initialize the ProductionFilter with the provided configuration.
        """
        super().__init__(config)
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
                defaults.get("movies_arg_spain_queue", "movies_arg_spain"),
            ]
        }

    def setup(self):
        """
        Setup method to initialize the filter.
        """
        self._initialize_queues()
        self._initialize_rabbitmq_processor()

    def _publish(self, queue, movie, headers):
        self.rabbitmq_processor.publish(target=queue, message=movie, headers=headers)
        logger.debug("Sent movie to %s", queue)

    def _handle_eos(self, queue_name, body, method, headers, client_state: ClientState):
        logger.debug("Received EOS from %s", queue_name)
        handle_eos(
            body,
            self.node_id,
            queue_name,
            self.source_queues,
            headers,
            self.nodes_of_type,
            self.rabbitmq_processor,
            client_state,
            target_queues=self.target_queues.get(queue_name),
        )
        self._free_resources(client_state)
        self.rabbitmq_processor.acknowledge(method)

    def _free_resources(self, client_state: ClientState):
        if client_state and client_state.has_received_all_eos(self.source_queues):
            self.client_manager.remove_client(client_state.client_id)

    def _process_movie(self, movie, queue_name, headers):
        country_dicts = movie.get("production_countries", [])
        country_names = [c.get("name") for c in country_dicts if "name" in c]

        if "Argentina" in country_names:
            self._publish(self.target_queues[queue_name][0], movie, headers)

        if len(country_names) == 1:
            self._publish(self.target_queues[queue_name][1], movie, headers)

        if "Argentina" in country_names and "Spain" in country_names:
            self._publish(self.target_queues[queue_name][2], movie, headers)

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to process individual movie messages from the input queue.
        """
        msg_type = self._get_message_type(properties)
        headers = getattr(properties, "headers", {}) or {}
        client_id = headers.get("client_id")

        if not client_id:
            logger.error("Missing client_id in headers")
            self.rabbitmq_processor.acknowledge(method)
            return

        client_state = self.client_manager.add_client(client_id, msg_type == EOS_TYPE)

        if msg_type == EOS_TYPE:
            self._handle_eos(queue_name, body, method, headers, client_state)
            return

        try:
            movie = self._decode_body(body, queue_name)
            if not movie:
                self.rabbitmq_processor.acknowledge(method)
                return

            if isinstance(movie, dict):
                movie = [movie]
            elif not isinstance(movie, list):
                logger.warning("Unexpected movie type: %s", type(movie))
                self.rabbitmq_processor.acknowledge(method)
                return

            for single_movie in movie:
                self._process_movie(single_movie, queue_name, headers)

        except Exception as e:
            logger.error("Error processing message from %s: %s", queue_name, e)

        finally:
            self.rabbitmq_processor.acknowledge(method)

    def process(self):
        """
        Main processing function for the ProductionFilter.
        It filters movies based on their production countries and sends them individually
        to the respective queues.
        """
        logger.info("ProductionFilter is starting up")
        self.run_consumer()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    production_filter = ProductionFilter(config)
    production_filter.setup()
    production_filter.process()
