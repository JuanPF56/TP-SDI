import configparser
import json

from common.logger import get_logger
logger = get_logger("Filter-Cleanup")

from common.filter_base import FilterBase, EOS_TYPE

class CleanupFilter(FilterBase):
    def __init__(self, config):
        """
        Initialize the CleanupFilter with the provided configuration.
        """
        super().__init__(config)
        self.batch = []
        self._initialize_queues()
        self._eos_flags = {q: False for q in self.source_queues}
        self._initialize_rabbitmq_processor()

    def _initialize_queues(self):
        defaults = self.config["DEFAULT"]

        self.source_queues = [
            defaults.get("movies_raw_queue", "movies_raw"),
            defaults.get("ratings_raw_queue", "ratings_raw"),
            defaults.get("credits_raw_queue", "credits_raw"),
        ]

        self.target_queues = {
            self.source_queues[0]: [
                defaults.get("movies_clean_for_production_queue", "movies_clean_for_production"),
                defaults.get("movies_clean_for_sentiment_queue", "movies_clean_for_sentiment"),
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
            "id", "original_title", "release_date", "budget",
            "revenue", "production_countries", "genres", "overview"
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
            logger.debug(f"Skipping invalid rating data: {data}")
            return None
        return {
            "movie_id": data["movieId"],
            "rating": data["rating"]
        }

    def clean_credit(self, data):
        """
        Callback function to process credit data to clean it.
        """
        required_fields = ["id", "cast"]
        if not all(data.get(field) is not None for field in required_fields):
            logger.debug(f"Skipping invalid credit data: {data}")
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

    def _mark_eos_received(self, body, queue_name, msg_type):
        """
        Mark the end-of-stream (EOS) flag for the specified queue.
        Up the count of the EOS message, if it is not the last node of type put it back to the input queue.
        If all source queues have received EOS, forward the EOS to the target queues.
        Args:
            body (str): The message body received from RabbitMQ.
            queue_name (str): The name of the source queue that received EOS.
            msg_type (str): The type of message received (should be "EOS").
        """
        try:
            if body:
                data = json.loads(body)
                count = data.get("count", 0)
            else:
                count = 0
        except json.JSONDecodeError:
            logger.error("Failed to decode EOS message")
            return
        
        # Send EOS back to the input queue for other cleanup nodes
        # only if this is not the last node of type
        if not self._eos_flags.get(queue_name):
            logger.debug(f"EOS marked for source queue: {queue_name}")
            self._eos_flags[queue_name] = True
            count += 1
        
        logger.debug(f"EOS count for queue {queue_name}: {count}")
        if count < self.nodes_of_type:
            logger.debug(f"Sending EOS back to input queue: {queue_name}")
            self.rabbitmq_processor.publish(
                queue=queue_name,
                message={"node_id": self.node_id, "count": count},
                msg_type=msg_type
            )

        targets = self.target_queues.get(queue_name)
        if targets:
            if isinstance(targets, list):
                for target in targets:
                    self.rabbitmq_processor.publish(
                        queue=target,
                        message={"node_id": self.node_id, "count": 0},
                        msg_type=msg_type
                    )
                    
                    logger.debug(f"EOS sent to target queue: {target}")
            else:
                self.rabbitmq_processor.publish(
                    queue=targets,
                    message={"node_id": self.node_id, "count": 0},
                    msg_type=msg_type
                )
                logger.debug(f"EOS sent to target queue: {targets}")
        
        if all(self._eos_flags.values()):
            logger.info("All source queues have sent EOS. Sending EOS to target queues.")
            self.rabbitmq_processor.stop_consuming()

    def _handle_eos(self, body, queue_name, method, msg_type):
        logger.debug(f"Received EOS from {queue_name}")
        if len(self.batch) > 0:
            logger.warning("Batch not empty when EOS received. Publishing remaining batch.")
            self._publish_batch(queue_name, self.batch, None)
            self.batch.clear()
        self._mark_eos_received(body, queue_name, msg_type)
        self.rabbitmq_processor.acknowledge(method)

    def _process_cleanup_batch(self, data_batch, queue_name):
        if queue_name == self.source_queues[0]:
            self.batch.extend([self.clean_movie(d) for d in data_batch])
        elif queue_name == self.source_queues[1]:
            self.batch.extend([self.clean_rating(d) for d in data_batch])
        elif queue_name == self.source_queues[2]:
            self.batch.extend([self.clean_credit(d) for d in data_batch])
        else:
            logger.warning(f"Unknown queue name: {queue_name}. Skipping.")

    def _fill_batch(self, queue_name, msg_type):
        batch_sz = self._determine_batch_size(queue_name)
        if self.batch and len(self.batch) >= batch_sz:
            self._publish_batch(queue_name, self.batch, msg_type)
            self.batch.clear()

    def _publish_batch(self, queue_name, batch, msg_type=None):
        target_queues = self.target_queues.get(queue_name, [])
        if not isinstance(target_queues, list):
            target_queues = [target_queues]
        for target_queue in target_queues:
            self.rabbitmq_processor.publish(
                queue=target_queue,
                message=batch,
                msg_type=msg_type
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

        if msg_type == EOS_TYPE:
            self._handle_eos(body, queue_name, method, msg_type)
            return

        try:
            data_batch = self._decode_body(body, queue_name)
            if data_batch is None:
                self.rabbitmq_processor.acknowledge(method)
                return

            self._process_cleanup_batch(data_batch, queue_name)
            self._fill_batch(queue_name, msg_type)

            self.rabbitmq_processor.acknowledge(method)

        except Exception as e:
            logger.error(f"Error processing message from {queue_name}: {e}")
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