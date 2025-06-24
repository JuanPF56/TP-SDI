import configparser
import os
from datetime import datetime

from common.client_state_manager import ClientManager
from common.election_logic import recover_node
from common.filter_base import FilterBase, EOS_TYPE
from common.eos_handling import handle_eos
from common.leader_election import LeaderElector
from common.master import REC_TYPE
from common.mom import RabbitMQProcessor
from common.logger import get_logger

logger = get_logger("Filter-Year")


class YearFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))
        self.join_rating_nodes = int(os.getenv("JOIN_RATING_NODES", "1"))
        self.join_credit_nodes = int(os.getenv("JOIN_CREDIT_NODES", "1"))

        self.client_manager = ClientManager(
            self.source_queues,
            manager=self.manager,
            nodes_to_await=self.eos_to_await,
        )

    def _initialize_rabbitmq_processor(self):
        all_target_queues = self.movie_table_target_queues + [self.target_queue]
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=all_target_queues,
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
        self.main_movie_table_target_queue = defaults.get(
            "movies_arg_post_2000_queue", "movies_arg_post_2000"
        )

        self.credits_target_queues = [
            self.main_movie_table_target_queue + "_node_join_credits_" + str(n_id) for n_id in range(1, self.join_credit_nodes + 1)
        ]

        self.ratings_target_queues = [
            self.main_movie_table_target_queue + "_node_join_ratings_" + str(n_id) for n_id in range(1, self.join_rating_nodes + 1)
        ]

        self.movie_table_target_queues = (
            self.credits_target_queues + self.ratings_target_queues
        )

    def setup(self):
        self._initialize_queues()
        self._initialize_rabbitmq_processor()
        self._initialize_master_logic()

    def _handle_eos(
        self, input_queue, body, method, headers
    ):
        queue = input_queue.split("_node_")[0]
        self.client_manager.handle_eos(
            body,
            self.node_id,
            queue,
            queue,
            headers,
            self.rabbitmq_processor,
            target_queues=(
                self.target_queue if input_queue == self.source_queues[1] else 
                self.movie_table_target_queues
            ),
        )

    def _free_resources(self, client_id):
        try:
            if self.client_manager.has_received_all_eos(client_id, self.main_source_queues):
                logger.info("All EOS received for client %s. Cleaning up resources.", client_id)
                self.client_manager.remove_client(client_id)
        except KeyError:
            logger.warning("Client not found for cleanup: %s.", client_id)

    def callback(self, ch, method, properties, body, input_queue):
        try:
            msg_type = self._get_message_type(properties)
            headers = getattr(properties, "headers", {}) or {}
            client_id = headers.get("client_id")
            message_id = headers.get("message_id")

            if msg_type == EOS_TYPE:
                self._handle_eos(input_queue, body, method, headers)
                return
            
            if msg_type == REC_TYPE:
                if self.elector is None:
                    self.elector = LeaderElector(self.node_id, self.peers, self.election_port, self._election_logic)
                    self.elector.start_election()
                return
            
            if client_id is None:
                logger.error("Missing client_id in headers")
                return
            
            if message_id is None:
                logger.error("Missing message_id in headers")
                return

            if self.duplicate_handler.is_duplicate(client_id, input_queue, message_id):
                logger.info("Duplicate message detected: %s. Acknowledging without processing.", message_id)
                return
            
            movie = self._decode_body(body, input_queue)
            if not movie:
                return

            if isinstance(movie, dict):
                movie = [movie]
            elif not isinstance(movie, list):
                logger.warning("Unexpected movie type: %s", type(movie))
                return

            processed_movies = []
            for single_movie in movie:
                processed = self._process_single_movie(single_movie, input_queue)
                if processed is not None:
                    processed_movies.append(processed)

            if processed_movies:
                self._publish_movie_batch(processed_movies, input_queue, headers)

            self.duplicate_handler.add(client_id, input_queue, message_id)
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
            # Argentina queue: publish to all movie table target queues
            for target_queue in self.movie_table_target_queues:
                self.rabbitmq_processor.publish(
                    target=target_queue,
                    message=movies,  
                    headers=headers,
                    priority=1,
                )
        elif input_queue == self.source_queues[1]:
            # Argentina+Spain queue: publish to queue
            self.rabbitmq_processor.publish(
                target=self.target_queue,
                message=movies, 
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
        if self.recovery_mode:
            recover_node(self, self.main_source_queues)
        else:
            self.elector = LeaderElector(self.node_id, self.peers, self.election_port, self._election_logic)
            self.elector.start_election()
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
