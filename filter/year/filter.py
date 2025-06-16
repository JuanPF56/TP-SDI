import configparser
import os
from datetime import datetime

from common.client_state_manager import ClientManager
from common.client_state_manager import ClientState
from common.filter_base import FilterBase, EOS_TYPE
from common.eos_handling import handle_eos
from common.mom import RabbitMQProcessor
from common.logger import get_logger

logger = get_logger("Filter-Year")


class YearFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))

        self.client_manager = ClientManager(
            expected_queues=self.source_queues,
            nodes_to_await=self.eos_to_await,
        )

    def _initialize_rabbitmq_processor(self):
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queue,
            target_exchange=self.target_exchange,
        )

    def _initialize_queues(self):
        defaults = self.config["DEFAULT"]
        
        self.main_source_queues = [
            defaults.get("movies_argentina_queue", "movies_argentina"),
            defaults.get("movies_arg_spain_queue", "movies_arg_spain"),
        ]

        self.source_queues = [
            queue + "_node_" + str(self.node_id) for queue in self.main_source_queues
        ]

        self.target_queue = defaults.get(
            "movies_arg_spain_2000s_queue", "movies_arg_spain_2000s"
        )
        self.target_exchange = defaults.get(
            "movies_arg_post_2000_exchange", "movies_arg_post_2000"
        )

    def setup(self):
        self._initialize_queues()
        self._initialize_rabbitmq_processor()
        self._initialize_master_logic()

    def _handle_eos(
        self, input_queue, body, method, headers, client_state: ClientState
    ):
        if client_state:
            logger.debug("Received EOS from %s", input_queue)
        handle_eos(
            body,
            self.node_id,
            input_queue,
            input_queue,
            headers,
            self.rabbitmq_processor,
            client_state,
            target_queues=(
                self.target_queue if input_queue == self.source_queues[1] else None
            ),
            target_exchanges=(
                self.target_exchange if input_queue == self.source_queues[0] else None
            ),
        )
        self._free_resources(client_state)
        self.rabbitmq_processor.acknowledge(method)

    def _free_resources(self, client_state: ClientState):
        if client_state and client_state.has_received_all_eos(self.source_queues):
            self.client_manager.remove_client(client_state.client_id)

    def callback(self, ch, method, properties, body, input_queue):
        msg_type = self._get_message_type(properties)
        headers = getattr(properties, "headers", {}) or {}
        client_id = headers.get("client_id")
        client_state = self.client_manager.add_client(client_id, msg_type == EOS_TYPE)

        if msg_type == EOS_TYPE:
            self._handle_eos(input_queue, body, method, headers, client_state)
            return

        try:
            movie = self._decode_body(body, input_queue)
            if not movie:
                self.rabbitmq_processor.acknowledge(method)
                return

            if isinstance(movie, dict):
                movie = [movie]
            elif not isinstance(movie, list):
                logger.warning("Unexpected movie type: %s", type(movie))
                self.rabbitmq_processor.acknowledge(method)
                return

            processed_movies = []
            for single_movie in movie:
                processed = self._process_single_movie(single_movie, input_queue)
                if processed is not None:
                    processed_movies.append(processed)

            if processed_movies:
                self._publish_movie_batch(processed_movies, input_queue, headers)

        except Exception as e:
            logger.error("Error processing message from %s: %s", input_queue, e)

        finally:
            self.rabbitmq_processor.acknowledge(method)

    def _process_single_movie(self, movie, input_queue):
        """
        Process a single movie and return it if it meets the criteria.
        Returns None if the movie doesn't meet the criteria.
        """
        title = movie.get("original_title")
        date_str = movie.get("release_date", "")
        release_year = self.extract_year(date_str)

        logger.debug(
            "Processing '%s' released in %s from queue '%s'",
            title,
            release_year,
            input_queue,
        )

        if input_queue == self.source_queues[0]:
            # Argentina queue: movies after 2000
            if release_year and release_year > 2000:
                return movie
        elif input_queue == self.source_queues[1]:
            # Argentina+Spain queue: movies between 2000-2009
            if release_year and 2000 <= release_year <= 2009:
                return movie
        else:
            logger.warning("Unknown source queue: %s", input_queue)
        
        return None

    def _publish_movie_batch(self, movies, input_queue, headers):
        """
        Publish all processed movies as one batch.
        """
        if input_queue == self.source_queues[0]:
            # Argentina queue: publish to exchange
            self.rabbitmq_processor.publish(
                target=self.target_exchange,
                message=movies,  
                exchange=True,
                headers=headers,
                priority=1,
            )
        elif input_queue == self.source_queues[1]:
            # Argentina+Spain queue: publish to queue
            self.rabbitmq_processor.publish(
                target=self.target_queue,
                message=movies, 
                exchange=False,
                headers=headers,
                priority=1,
            )

    def process(self):
        """
        Reads from:
        - movies_argentina
        - movies_arg_spain

        Writes to:
        - movies_arg_post_2000: Argentine-only movies after 2000
        - movies_arg_spain_2000s: Argentina+Spain movies between 2000-2009
        """
        logger.info("YearFilter is starting up")
        self.run_consumer()

    def extract_year(self, date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").year
        except Exception as e:
            logger.warning("Invalid release_date '%s': %s", date_str, e)
            return None


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    year_filter = YearFilter(config)
    year_filter.setup()
    year_filter.process()
