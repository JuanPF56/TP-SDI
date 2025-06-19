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
        self.main_source_queues = [defaults.get("movies_clean_queue", "movies_clean")]
        self.source_queues = [queue + "_node_" + str(self.node_id) for queue in self.main_source_queues]
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
        self._initialize_master_logic()

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
            self.rabbitmq_processor,
            client_state,
            target_queues=self.target_queues.get(queue_name),
        )
        self._free_resources(client_state)

    def _free_resources(self, client_state: ClientState):
        if client_state and client_state.has_received_all_eos(self.source_queues):
            self.client_manager.remove_client(client_state.client_id)

    def _process_single_movie(self, movie, queue_name):
        """
        Process a single movie and return categorized results.
        Returns a dict with categories as keys and the movie as value if it matches the category.
        """
        country_dicts = movie.get("production_countries", [])
        country_names = [c.get("name") for c in country_dicts if "name" in c]
        
        results = {}
        
        if "Argentina" in country_names:
            results['argentina'] = movie

        if len(country_names) == 1:
            results['single_country'] = movie

        if "Argentina" in country_names and "Spain" in country_names:
            results['argentina_spain'] = movie
        
        return results

    def _publish_movie_batches(self, categorized_movies, queue_name, headers):
        """
        Publish categorized movies in batches to their respective target queues.
        """
        target_queues = self.target_queues[queue_name]
        
        # Publish Argentina movies to movies_argentina queue
        if categorized_movies['argentina']:
            self._publish(target_queues[0], categorized_movies['argentina'], headers)  # movies_argentina
        
        # Publish single country movies to movies_solo queue
        if categorized_movies['single_country']:
            self._publish(target_queues[1], categorized_movies['single_country'], headers)  # movies_solo
        
        # Publish Argentina+Spain movies to movies_arg_spain queue
        if categorized_movies['argentina_spain']:
            self._publish(target_queues[2], categorized_movies['argentina_spain'], headers)  # movies_arg_spain

    def callback(self, ch, method, properties, body, queue_name):
        """
        Callback function to process individual movie messages from the input queue.
        """

        try:
            msg_type = self._get_message_type(properties)
            headers = getattr(properties, "headers", {}) or {}
            client_id = headers.get("client_id")
            message_id = headers.get("message_id")

            if not client_id:
                logger.error("Missing client_id in headers")
                return

            client_state = self.client_manager.add_client(client_id, msg_type == EOS_TYPE)

            if msg_type == EOS_TYPE:
                self._handle_eos(queue_name, body, method, headers, client_state)
                return
            
            if message_id is None:
                logger.error("Missing message_id in headers")
                return
            
            if self.duplicate_handler.is_duplicate(client_id, queue_name, message_id):
                logger.info("Duplicate message detected: %s. Acknowledging without processing.", message_id)
                return
        
            movie = self._decode_body(body, queue_name)
            if not movie:
                return

            if isinstance(movie, dict):
                movie = [movie]
            elif not isinstance(movie, list):
                logger.warning("Unexpected movie type: %s", type(movie))
                return

            # Process all movies first and categorize them
            categorized_movies = {
                'argentina': [],
                'single_country': [],
                'argentina_spain': []
            }
            
            for single_movie in movie:
                results = self._process_single_movie(single_movie, queue_name)
                
                if 'argentina' in results:
                    categorized_movies['argentina'].append(results['argentina'])
                
                if 'single_country' in results:
                    categorized_movies['single_country'].append(results['single_country'])
                
                if 'argentina_spain' in results:
                    categorized_movies['argentina_spain'].append(results['argentina_spain'])

            # Publish all categorized movies in batches
            self._publish_movie_batches(categorized_movies, queue_name, headers)

            self.duplicate_handler.add(client_id, queue_name, message_id)
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
        self.elector.start_election()
        self.run_consumer()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    production_filter = ProductionFilter(config)
    production_filter.setup()
    production_filter.process()
