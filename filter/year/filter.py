import configparser
import json
import os
from datetime import datetime

from common.logger import get_logger
logger = get_logger("Filter-Year")

from common.filter_base import FilterBase, EOS_TYPE

class YearFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1")) * 2  # Two queues to await EOS from

        self._initialize_queues()
        self._initialize_rabbitmq_processor()

    def _initialize_queues(self):
        defaults = self.config["DEFAULT"]
        
        self.source_queues = [
            defaults.get("movies_argentina_queue", "movies_argentina"),
            defaults.get("movies_arg_spain_queue", "movies_arg_spain")
        ]

        self.target_queues = {
            self.source_queues[0]: defaults.get("movies_arg_post_2000_queue", "movies_arg_post_2000"),
            self.source_queues[1]: defaults.get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")
        }

        self.processed_batch = {
            self.source_queues[0]: [],
            self.source_queues[1]: []
        }

        self._eos_flags = {
            self.source_queues[0]: {},
            self.source_queues[1]: {}
        }

    def setup(self):
        self._initialize_queues()
        self._initialize_rabbitmq_processor()

    def _mark_eos_received(self, body, input_queue):
        """
        Mark the end of stream (EOS) for the given input queue
        for the given node.
        """
        try:
            data = json.loads(body)
            node_id = data.get("node_id")
            count = data.get("count", 0)
        except json.JSONDecodeError:
            logger.error("Failed to decode EOS message")
            return
        if input_queue not in self._eos_flags:
            self._eos_flags[input_queue] = {}
        if node_id not in self._eos_flags[input_queue]:
            count += 1
        logger.info(f"EOS received for node {node_id} from input queue {input_queue}")
        self._eos_flags[input_queue][node_id] = True
        # If this isn't the last node, send the EOS message back to the input queue
        logger.debug(f"EOS count for node {node_id}: {count}")
        if count < self.nodes_of_type: 
            # Send EOS back to input queue for other year nodes
            self.rabbitmq_processor.publish(
                queue=input_queue,
                message={"node_id": node_id, "count": count},
                msg_type=EOS_TYPE
            )
    
    def _check_eos_flags(self):
        """
        Check if all nodes have sent EOS and propagate to output queues.
        """
        eos_nodes_len = len(self._eos_flags[self.source_queues[0]]) + len(self._eos_flags[self.source_queues[1]])
        all_eos_received = all(self._eos_flags[input_queue].get(node) for input_queue in self.source_queues for node in self._eos_flags[input_queue])
        logger.debug(f"EOS flags: {self._eos_flags}")
        logger.debug(f"EOS to await: {self.eos_to_await}")
        logger.debug(f"EOS nodes length: {eos_nodes_len}")
        logger.debug(f"EOS all received: {all_eos_received}")
        if all_eos_received and eos_nodes_len == int(self.eos_to_await):
            logger.info("All nodes have sent EOS. Sending EOS to output queues.")
            self._send_eos()
            self.rabbitmq_processor.stop_consuming()
        else:
            logger.debug("Not all nodes have sent EOS yet. Waiting...")

    def _send_eos(self):
        """
        Propagate the end of stream (EOS) to all output queues.
        """
        logger.debug("Sending EOS to output queues")
        for queue in self.target_queues.values():
            self.rabbitmq_processor.publish(
                queue=queue,
                message={"node_id": self.node_id, "count": 0},
                msg_type=EOS_TYPE,
            )
            logger.info(f"EOS message sent to {queue}")


    def callback(self, ch, method, properties, body, input_queue):
        msg_type = self._get_message_type(properties)

        if msg_type == EOS_TYPE:
            self._mark_eos_received(body, input_queue)
            # Publish any remaining processed movies
            if len(self.processed_batch[input_queue]) > 0:
                self.rabbitmq_processor.publish(
                    queue=self.target_queues[input_queue],
                    message=self.processed_batch[input_queue],
                )
                self.processed_batch[input_queue] = []
            
            self._check_eos_flags()
            self.rabbitmq_processor.acknowledge(method)
            return

        try:
            movies_batch = json.loads(body)
            if not isinstance(movies_batch, list):
                logger.warning("❌ Expected a list (batch) of movies, skipping.")
                self.rabbitmq_processor.acknowledge(method)
                return

            for movie in movies_batch:
                title = movie.get("original_title")
                date_str = movie.get("release_date", "")
                release_year = self.extract_year(date_str)

                logger.debug(f"Processing '{title}' released in {release_year} from queue '{input_queue}'")

                if input_queue == self.source_queues[0]:
                    # Argentine movies after 2000
                    if release_year and release_year > 2000:
                        self.processed_batch[input_queue].append(movie)
                elif input_queue == self.source_queues[1]:
                    # Argentina+Spain movies in the 2000s decade
                    if release_year and 2000 <= release_year <= 2009:
                        self.processed_batch[input_queue].append(movie)
                else:
                    logger.warning(f"Unknown source queue: {input_queue}")

            # Publish the processed batch when it reaches the batch size

            if len(self.processed_batch[input_queue]) >= self.batch_size:
                logger.info(f"Publishing {len(self.processed_batch[input_queue])} movies to {self.target_queues[input_queue]}")
                self.rabbitmq_processor.publish(
                    queue=self.target_queues[input_queue],
                    message=self.processed_batch[input_queue],
                    msg_type=msg_type
                )
                self.processed_batch[input_queue] = []

            self.rabbitmq_processor.acknowledge(method)

        except json.JSONDecodeError:
            logger.warning("❌ Skipping invalid JSON")
            self.rabbitmq_processor.acknowledge(method)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.rabbitmq_processor.acknowledge(method)

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
            logger.warning(f"Invalid release_date '{date_str}': {e}")
            return None

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    year_filter = YearFilter(config)
    year_filter.setup()
    year_filter.process()
