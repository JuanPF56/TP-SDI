import json
import os
import time
import pika
import multiprocessing
import signal

EOS_TYPE = "EOS"

class JoinBatchBase:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.channel = None
        delay = 2 # Seconds
        while True:
            try:
                rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
                self.channel = self.connection.channel()
                print("Connected to RabbitMQ")
                break
            except pika.exceptions.AMQPConnectionError:
                print("RabbitMQ connection error. Retrying in 5 seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
        # Get the movies table exchange name from the config
        movies_table_exchange = self.config["DEFAULT"].get("movies_table_exchange", "movies_table_broadcast")
        # Get the clean batch queue name from the config
        self.input_queue = self.config["DEFAULT"].get("input_queue", "input_queue")
        self.output_queue = self.config["DEFAULT"].get("output_queue", "output_queue")

        # Declare a fanout exchange
        self.channel.exchange_declare(exchange=movies_table_exchange, exchange_type='fanout')
        # Create a new queue with a random name
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.table_queue = result.method.queue
        # Bind the queue to the exchange
        self.channel.queue_bind(exchange=movies_table_exchange, queue=self.table_queue)
        
        # Declare the input queue
        self.channel.queue_declare(queue=self.input_queue)
        # Declare the output queue
        self.channel.queue_declare(queue=self.output_queue)
        
        # Create a shared list to store the movies table
        self.manager = multiprocessing.Manager()
        self.movies_table = self.manager.list()
        self.movies_table_ready = self.manager.Event()

        self.table_receiver = multiprocessing.Process(target=self.receive_movies_table)

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)

    def __handleSigterm(self, signum, frame):
        print("SIGTERM signal received. Closing connection...")
        self.connection.close()
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
        # Callback function to handle incoming messages
        def callback(ch, method, properties, body):
            # Process the incoming message (the movies table)
            movies_table = body.decode('utf-8')
            movies_table = json.loads(movies_table)

            # Update the shared movies table with the new data
            self.movies_table.extend(movies_table["movies"])
            log_message = f"Received movies table: {len(movies_table['movies'])} movies"
            self.log_info(log_message)
            # Notify that the movies table is ready
            self.movies_table_ready.set()
            self.channel.stop_consuming()
        
        self.log_info("Waiting for movies table...")
        # Start consuming messages from the queue
        self.channel.basic_consume(queue=self.table_queue, on_message_callback=callback, auto_ack=True)
        
        self.channel.start_consuming()

    def receive_batch(self):
        # Wait for the movies table to be received
        self.movies_table_ready.wait()
        
        self.log_info("Movies table is ready. Starting to receive batches...")
        self.channel.basic_consume(
            queue=self.input_queue,
            on_message_callback=self.process_batch,
            auto_ack=True
        )

        self.channel.start_consuming()

    def process_batch(self, ch, method, properties, body):
        raise NotImplementedError("Subclasses must implement this method")
    
    def log_info(self, message):
        raise NotImplementedError("Subclasses must implement this method")