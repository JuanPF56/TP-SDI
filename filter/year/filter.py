import configparser
import json
import os
from datetime import datetime

from common.logger import get_logger
from common.mom import RabbitMQProcessor
logger = get_logger("Filter-Year")

from common.filter_base import FilterBase, EOS_TYPE

class YearFilter(FilterBase):
    def __init__(self, config):
        super().__init__(config)
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1")) * 2  # Two queues to await EOS from

        self._initialize_queues()
        self._initialize_rabbitmq_processor()

    def _initialize_rabbitmq_processor(self):
        self.rabbitmq_processor = RabbitMQProcessor(
            config=self.config,
            source_queues=self.source_queues,
            target_queues=self.target_queue,
            target_exchange=self.target_exchange          
        )

    def _initialize_queues(self):
        defaults = self.config["DEFAULT"]
        
        self.source_queues = [
            defaults.get("movies_argentina_queue", "movies_argentina"),
            defaults.get("movies_arg_spain_queue", "movies_arg_spain")
        ]

        self.target_queue = defaults.get("movies_arg_spain_2000s_queue", "movies_arg_spain_2000s")
        self.target_exchange = defaults.get("movies_arg_post_2000_exchange", "movies_arg_post_2000")

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

    def _mark_eos_received(self, body, input_queue, headers):
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
                target=input_queue,
                message={"node_id": node_id, "count": count},
                msg_type=EOS_TYPE,
                headers=headers
            )
    
    def _check_eos_flags(self, headers):
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
            self._send_eos(headers)
            self.rabbitmq_processor.stop_consuming()
        else:
            logger.debug("Not all nodes have sent EOS yet. Waiting...")

    def _send_eos(self, headers):
        """
        Propagate the end of stream (EOS) to all output queues.
        """
        logger.debug("Sending EOS to output queue and exchange")
        self.rabbitmq_processor.publish(
            target=self.target_queue,
            message={"node_id": self.node_id, "count": 0},
            msg_type=EOS_TYPE,
            headers=headers
        )
        logger.info(f"EOS message sent to {self.target_queue}")
        self.rabbitmq_processor.publish(
            target=self.target_exchange,
            message={"node_id": self.node_id, "count": 0},
            msg_type=EOS_TYPE,
            exchange=True,
            headers=headers
        )
        logger.info(f"EOS message sent to {self.target_exchange}")

    def _handle_eos(self, input_queue, body, method, headers):
        logger.debug(f"Received EOS from {input_queue}")

        if self.processed_batch.get(input_queue):
            exchange = False
            if input_queue == self.source_queues[0]:
                # Argentine-only movies after 2000
                logger.info(f"Publishing {len(self.processed_batch[input_queue])} movies to {self.target_exchange}")   
                exchange = True
            elif input_queue == self.source_queues[1]:
                # Argentina + Spain movies between 2000-2009
                logger.info(f"Publishing {len(self.processed_batch[input_queue])} movies to {self.target_queue}")
            else:
                logger.warning(f"Unknown source queue: {input_queue}")
                return
            self.rabbitmq_processor.publish(
                target=self.target_exchange if exchange else self.target_queue,
                message=self.processed_batch[input_queue],
                exchange=exchange,
                headers=headers
            )
            self.processed_batch[input_queue] = []

        self._mark_eos_received(body, input_queue, headers)
        self._check_eos_flags(headers)
        self.rabbitmq_processor.acknowledge(method)

    def _process_movies_batch(self, movies_batch, input_queue):
        for movie in movies_batch:
            title = movie.get("original_title")
            date_str = movie.get("release_date", "")
            release_year = self.extract_year(date_str)

            logger.debug(f"Processing '{title}' released in {release_year} from queue '{input_queue}'")

            if input_queue == self.source_queues[0]:
                # Argentine-only movies after 2000
                if release_year and release_year > 2000:
                    self.processed_batch[input_queue].append(movie)
            elif input_queue == self.source_queues[1]:
                # Argentina + Spain movies between 2000-2009
                if release_year and 2000 <= release_year <= 2009:
                    self.processed_batch[input_queue].append(movie)
            else:
                logger.warning(f"Unknown source queue: {input_queue}")

    def _publish_ready_batches(self, input_queue, msg_type, headers):
        if len(self.processed_batch[input_queue]) >= self.batch_size:
            exchange = False
            if input_queue == self.source_queues[0]:
                # Argentine-only movies after 2000
                logger.info(f"Publishing {len(self.processed_batch[input_queue])} movies to {self.target_exchange}")   
                exchange = True
            elif input_queue == self.source_queues[1]:
                # Argentina + Spain movies between 2000-2009
                logger.info(f"Publishing {len(self.processed_batch[input_queue])} movies to {self.target_queue}")
            else:
                logger.warning(f"Unknown source queue: {input_queue}")
                return
            self.rabbitmq_processor.publish(
                target=self.target_exchange if exchange else self.target_queue,
                message=self.processed_batch[input_queue],
                msg_type=msg_type,
                exchange=exchange,
                headers=headers
            )
            self.processed_batch[input_queue] = []


    def callback(self, ch, method, properties, body, input_queue):
        msg_type = self._get_message_type(properties)
        headers = getattr(properties, "headers", {}) or {}

        if msg_type == EOS_TYPE:
            self._handle_eos(input_queue, body, method, headers)
            return

        try:
            movies_batch = self._decode_body(body, input_queue)
            if not movies_batch:
                self.rabbitmq_processor.acknowledge(method)
                return

            self._process_movies_batch(movies_batch, input_queue)
            self._publish_ready_batches(input_queue, msg_type, headers)

        except Exception as e:
            logger.error(f"Error processing message from {input_queue}: {e}")

        finally:
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
