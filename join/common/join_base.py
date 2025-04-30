import json
import os
import time
import pika
import multiprocessing
import signal

EOS_TYPE = "EOS"

SECONDS_TO_HEARTBEAT = 600  # 10 minutes (adjustable)

class JoinBase:
    def __init__(self, config):
        """
        Initialize the JoinBase class with the given configuration.
        """
        self.config = config
        # Connect to RabbitMQ with retry
        self.connection, self.channel = self.__connect_to_rabbitmq()

        # Get the clean batch queue name from the config
        self.input_queue = self.config["DEFAULT"].get("input_queue", "input_queue")
        self.output_queue = self.config["DEFAULT"].get("output_queue", "output_queue")
        
        # Declare the input queue
        self.channel.queue_declare(queue=self.input_queue)
        # Declare the output queue
        self.channel.queue_declare(queue=self.output_queue)

        # Get the EOS to await
        self.node_id = int(os.getenv("NODE_ID", "1"))
        self.eos_to_await = int(os.getenv("NODES_TO_AWAIT", "1"))
        self.nodes_of_type = int(os.getenv("NODES_OF_TYPE", "1"))
        self.year_nodes_to_await = int(os.getenv("YEAR_NODES_TO_AWAIT", "1"))

        self._eos_flags = {}
        self._year_eos_flags = {}
        
        # Create a shared list to store the movies table
        self.manager = multiprocessing.Manager()
        self.movies_table = self.manager.list()
        self.movies_table_ready = self.manager.Event()

        self.table_receiver = multiprocessing.Process(target=self.receive_movies_table)

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)

    def __connect_to_rabbitmq(self):
        connection = None
        channel = None
        delay = 2  # Seconds
        while True:
            try:
                rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
                # Set heartbeat interval
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=rabbitmq_host,
                        heartbeat=SECONDS_TO_HEARTBEAT
                    )
                )
                channel = connection.channel()
                log_message = f"Process {os.getpid()} connected to RabbitMQ at {rabbitmq_host}"
                self.log_info(log_message)
                break
            except pika.exceptions.AMQPConnectionError:
                log_message = f"Process {os.getpid()} RabbitMQ connection error. Retrying in {delay} seconds..."
                self.log_info(log_message)
                time.sleep(delay)
                delay *= 2  # Exponential backoff
        return connection, channel

    def __handleSigterm(self, signum, frame):
        print("SIGTERM signal received. Closing connection...")
        try:
            self.connection.close()
        except Exception as e:
            self.log_info(f"Error closing connection: {e}")
        finally:
            self.channel.stop_consuming()
            self.channel.close()
            self.manager.shutdown()
            os.kill(self.table_receiver.pid, signal.SIGTERM)
            self.table_receiver.join()

    def process(self):
        # Start the process to receive the movies table
        self.table_receiver.start()

        # Start the loop to receive the batches
        self.receive_batch()

        self.table_receiver.join()

    def receive_movies_table(self):
        """
        Start the process to receive the movies from the broadcast exchange.
        This method will create a new queue and bind it to the exchange.
        It will then consume messages from the queue and populate the movies table.
        Once all nodes have sent EOS messages, it will notify that the movies table is ready.
        """
        # Get the movies table exchange name from the config
        movies_exchange = self.config["DEFAULT"].get("movies_exchange", "movies_arg_post_2000")
        # Connect to RabbitMQ
        conn, chan = self.__connect_to_rabbitmq()

        def hdlSigTermTableRcv(signum, frame):
            chan.stop_consuming()
            chan.close()

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, hdlSigTermTableRcv)

        # Declare a fanout exchange
        chan.exchange_declare(exchange=movies_exchange, exchange_type='fanout')
        # Form the queue name
        # movies_queue = 'movies_queue_' + str(self.node_id)
        # Create a new queue for the movies
        result = chan.queue_declare(queue='', exclusive=True)
        # Bind the queue to the exchange
        movies_queue = result.method.queue
        chan.queue_bind(exchange=movies_exchange, queue=movies_queue)

        def callback(ch, method, properties, body):
            msg_type = properties.type if properties and properties.type else "UNKNOWN"

            if msg_type == EOS_TYPE:
                try:
                    data = json.loads(body)
                    node_id = data.get("node_id")
                except json.JSONDecodeError:
                    self.log_error("Failed to decode EOS message")
                    return
                if node_id not in self._year_eos_flags:
                    self._year_eos_flags[node_id] = True
                    self.log_debug(f"EOS received for node {node_id}.")
                if len(self._year_eos_flags) == int(self.year_nodes_to_await):
                    self.log_info("All nodes have sent EOS. Notifying movies table ready.")
                    self.movies_table_ready.set()
                    ch.stop_consuming()
            else:
                try:
                    movies = json.loads(body)
                except json.JSONDecodeError:
                    self.log_error(f"Error decoding JSON: {body}")
                    return
                self.log_debug(f"Received message: {movies}")
                for movie in movies:
                    new_movie = {
                        "id" : str(movie["id"]),
                        "original_title": movie["original_title"],
                    }
                    self.movies_table.append(new_movie)
                self.log_debug(f"Received {len(movies)} movies so far.")
                
        self.log_info("Consuming from queue: movies_queue")
        chan.basic_consume(queue=movies_queue, on_message_callback=callback, auto_ack=True)
                
        chan.start_consuming()
        chan.close()

    def receive_batch(self):
        """
        Start the process to receive batches from the input queue.
        This method will wait for the movies table to be ready before 
        starting to consume messages.
        """
        # Wait for the movies table to be ready
        self.movies_table_ready.wait()

        self.log_info("Movies table is ready. Starting to receive batches...")

        while True:
            try:
                self.channel.basic_consume(
                    queue=self.input_queue,
                    on_message_callback=self.process_batch,
                    auto_ack=True
                )
                self.channel.start_consuming()

            except pika.exceptions.StreamLostError as e:
                self.log_info(f"[WARNING] Stream lost. Reconnecting... Reason: {e}")
                time.sleep(1)
                self.connection, self.channel = self.__connect_to_rabbitmq()

            except pika.exceptions.AMQPConnectionError as e:
                self.log_info(f"[WARNING] AMQP Connection error: {e}. Retrying in 2s...")
                time.sleep(2)
                self.connection, self.channel = self.__connect_to_rabbitmq()

            except Exception as e:
                self.log_info(f"[ERROR] Unexpected error in receive_batch: {e}")
                break

    def process_batch(self, ch, method, properties, body):
        """
        Process the incoming batch of messages.
        This method handles the end-of-stream (EOS) messages and performs the join operation.
        The join operation is defined in the perform_join method, which should be overridden
        by subclasses.
        """
        try:
            msg_type = properties.type if properties and properties.type else "UNKNOWN"
            if msg_type == "EOS":
                try:
                    data = json.loads(body)
                    node_id = data.get("node_id")
                    count = data.get("count")
                except json.JSONDecodeError:
                    self.log_debug("Failed to decode EOS message")
                    return
                self.log_debug(f"EOS message received: {data}")
                if node_id not in self._eos_flags:
                    count += 1
                    self._eos_flags[node_id] = True
                    self.log_debug(f"EOS received for node {node_id}.")
                if len(self._eos_flags) == int(self.eos_to_await):
                    self.log_info("All nodes have sent EOS. Sending EOS to output queue.")
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.output_queue,
                        body=json.dumps({"node_id": self.node_id, "count": 0}),
                        properties=pika.BasicProperties(type=msg_type)
                    )
                    ch.stop_consuming()
                self.log_debug(f"EOS count for node {node_id}: {count}")
                self.log_debug(f"Nodes of type: {self.nodes_of_type}")
                # If this isn't the last node, put the EOS message back to the queue for other nodes
                if count < self.nodes_of_type:
                    self.log_debug(f"Sending EOS back to input queue for node {node_id}.")
                    # Put the EOS message back to the queue for other nodes
                    self.channel.basic_publish(
                        exchange='',
                        routing_key=self.input_queue,
                        body=json.dumps({"node_id": node_id, "count": count}),
                        properties=pika.BasicProperties(type=msg_type)
                    )
                return
            # Load the data from the incoming message
            try:
                decoded = json.loads(body)
                if isinstance(decoded, list):
                    self.log_debug(f"Received list: {decoded}")
                    data = decoded
                elif isinstance(decoded, dict):
                    self.log_debug(f"Received dict: {decoded}")
                    data = [decoded]
                else:
                    self.log_warning(f"Unexpected JSON format: {decoded}")
                    return
            except json.JSONDecodeError as e:
                self.log_error(f"Error decoding JSON: {e}")
                return
            
            # Build a set of movie IDs for fast lookup
            movies_by_id = {movie["id"]: movie for movie in self.movies_table}
            joined_data = self.perform_join(data, movies_by_id)            

            if not joined_data:
                self.log_debug("No matching movies found in the movies table.")
                return
            
            self.channel.basic_publish(
                exchange='',
                routing_key=self.output_queue,
                body=json.dumps(joined_data).encode('utf-8'),
                properties=pika.BasicProperties(type=msg_type)
            )
        except pika.exceptions.StreamLostError as e:
            self.log_info(f"Stream lost, reconnecting: {e}")
            self.connection.close()
            self.channel.stop_consuming()
            self.connection, self.channel = self.__connect_to_rabbitmq()
            self.receive_batch()

        except Exception as e:
            self.log_error(f"[ERROR] Unexpected error in process_batch: {e}")
    
    def perform_join(self, data, movies_by_id):
        """
        Perform the join operation between the incoming data and the movies table.
        This method should be overridden by subclasses to implement specific join logic.
        """
        raise NotImplementedError("Subclasses must implement this method")

    def log_info(self, message):
        raise NotImplementedError("Subclasses must implement this method")
    
    def log_error(self, message):
        raise NotImplementedError("Subclasses must implement this method")
    
    def log_debug(self, message):
        raise NotImplementedError("Subclasses must implement this method")
    
    def log_warning(self, message):
        raise NotImplementedError("Subclasses must implement this method")