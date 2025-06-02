import configparser
from collections import defaultdict

from common.client_state_manager import ClientManager
from common.client_state import ClientState
from common.eos_handling import handle_eos
from common.filter_base import FilterBase, EOS_TYPE

from common.logger import get_logger

logger = get_logger("Filter-Cleanup")


class CleanupFilter(FilterBase):
    def __init__(self, config):
        """
        Initialize the CleanupFilter with the provided configuration.
        """
        super().__init__(config)
        self._initialize_queues()
        self.batches = defaultdict(list)
        self._initialize_rabbitmq_processor()
        self.client_manager = ClientManager(self.source_queues)

    def _initialize_queues(self):
        defaults = self.config["DEFAULT"]

        self.source_queues = [
            defaults.get("movies_raw_queue", "movies_raw"),
            defaults.get("ratings_raw_queue", "ratings_raw"),
            defaults.get("credits_raw_queue", "credits_raw"),
        ]

        self.target_queues = {
            self.source_queues[0]: [
                defaults.get(
                    "movies_clean_for_production_queue", "movies_clean_for_production"
                ),
                defaults.get(
                    "movies_clean_for_sentiment_queue", "movies_clean_for_sentiment"
                ),
            ],
            self.source_queues[1]: defaults.get("ratings_clean_queue", "ratings_clean"),
            self.source_queues[2]: defaults.get("credits_clean_queue", "credits_clean"),
        }

    def setup(self):
        """
        Setup method to initialize the filter.
        This method is called when the filter is instantiated.
        It sets up the source and target queues, and initializes the RabbitMQ processor.
        """
        self._initialize_queues()
        self._eos_flags = {q: False for q in self.source_queues}
        self._initialize_rabbitmq_processor()

    def clean_movie(self, data):
        """
        Callback function to process movie data to clean it.
        """
        required_fields = [
            "id",
            "original_title",
            "release_date",
            "budget",
            "revenue",
            "production_countries",
            "genres",
            "overview",
        ]
        if not all(data.get(field) is not None for field in required_fields):
            return None
        return {field: data[field] for field in required_fields}

    def clean_rating(self, data):
        """
        Callback function to process rating data to clean it.
        """
        required_fields = ["userId", "movieId", "rating"]
        if not all(data.get(field) is not None for field in required_fields):
            logger.debug("Skipping invalid rating data: %r", data)
            return None
        return {"movie_id": data["movieId"], "rating": data["rating"]}

    def clean_credit(self, data):
        """
        Callback function to process credit data to clean it.
        """
        required_fields = ["id", "cast"]
        if not all(data.get(field) is not None for field in required_fields):
            logger.debug("Skipping invalid credit data: %r", data)
            return None

        cast = []
        if data["cast"]:
            for actor in data["cast"]:
                if actor.get("name"):
                    cast.append(actor["name"])

        return {
            "id": data["id"],
            "cast": cast,
        }

    def _handle_eos(self, queue_name, body, method, headers, client_state: ClientState):
        if client_state:
            key = client_state.client_id
            if self.batches[key] and len(self.batches[key]) > 0:
                logger.warning(
                    "Batch not empty when EOS received. Publishing remaining batch."
                )
                self._publish_batch(queue_name, self.batches[key], headers, None)
                self.batches[key].clear()
        handle_eos(
            body,
            self.node_id,
            queue_name,
            queue_name,
            headers,
            self.nodes_of_type,
            self.rabbitmq_processor,
            client_state,
            target_queues=self.target_queues.get(queue_name),
        )
        self._free_resources(client_state)
        self.rabbitmq_processor.acknowledge(method)

    def _free_resources(self, client_state: ClientState):
        try:
            if client_state and client_state.has_received_all_eos(self.source_queues):
                del self.batches[(client_state.client_id)]
                self.client_manager.remove_client(client_state.client_id)
        except KeyError:
            logger.warning("Batch not found for client %s.", client_state.client_id)

    def _process_cleanup_batch(self, data_batch, queue_name, client_state: ClientState):
        key = client_state.client_id
        if queue_name == self.source_queues[0]:
            self.batches[key].extend([self.clean_movie(d) for d in data_batch])
        elif queue_name == self.source_queues[1]:
            self.batches[key].extend([self.clean_rating(d) for d in data_batch])
        elif queue_name == self.source_queues[2]:
            self.batches[key].extend([self.clean_credit(d) for d in data_batch])
        else:
            logger.warning("Unknown queue name: %s. Skipping.", queue_name)

    def _publish_ready_batches(
        self, queue_name, msg_type, headers, client_state: ClientState
    ):
        key = client_state.client_id
        batch_sz = self._determine_batch_size(queue_name)
        if self.batches[key] and len(self.batches[key]) >= batch_sz:
            self._publish_batch(queue_name, self.batches[key], headers, msg_type)
            self.batches[key].clear()

    def _publish_batch(self, queue_name, batch, headers, msg_type=None):
        target_queues = self.target_queues.get(queue_name, [])
        if not isinstance(target_queues, list):
            target_queues = [target_queues]
        for target_queue in target_queues:
            self.rabbitmq_processor.publish(
                target=target_queue, message=batch, msg_type=msg_type, headers=headers
            )

    def _determine_batch_size(self, queue_name):
        if queue_name == self.source_queues[1]:
            return 10000
        if queue_name == self.source_queues[0]:
            return 50
        return self.batch_size

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to handle incoming messages from RabbitMQ.
        Handles EOS and batch message processing/publishing.
        """
        msg_type = self._get_message_type(properties)
        headers = getattr(properties, "headers", {}) or {}
        client_id = headers.get("client_id")
        client_state = self.client_manager.add_client(client_id, msg_type == EOS_TYPE)

        if msg_type == EOS_TYPE:
            self._handle_eos(queue_name, body, method, headers, client_state)
            return

        try:
            data_batch = self._decode_body(body, queue_name)
            if data_batch is None:
                self.rabbitmq_processor.acknowledge(method)
                return

            self._process_cleanup_batch(data_batch, queue_name, client_state)
            self._publish_ready_batches(queue_name, msg_type, headers, client_state)

        except Exception as e:
            logger.error("Error processing message from %s: %s", queue_name, e)

        finally:
            self.rabbitmq_processor.acknowledge(method)

    def process(self):
        """
        Main processing function for the CleanupFilter.
        """
        logger.info("CleanupFilter is starting up")
        self.run_consumer()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    filter_instance = CleanupFilter(config)
    filter_instance.setup()
    filter_instance.process()
