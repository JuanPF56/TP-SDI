import json
import os
import time
import pika
import multiprocessing
import signal

class JoinBatchBase:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.channel = None
        while True:
            try:
                rabbitmq_host = self.config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
                self.channel = self.connection.channel()
                print("Connected to RabbitMQ")
                break
            except pika.exceptions.AMQPConnectionError:
                print("RabbitMQ connection error. Retrying in 5 seconds...")
                time.sleep(5)
        # Get the movies table exchange name from the config
        movies_table_exchange = self.config["DEFAULT"].get("movies_table_exchange", "movies_table_broadcast")
        # Declare a fanout exchange
        self.channel.exchange_declare(exchange=movies_table_exchange, exchange_type='fanout', durable=True)
        # Create a new queue with a random name
        result = self.channel.queue_declare(queue='', exclusive=True, durable=True)
        self.queue_name = result.method.queue
        # Bind the queue to the exchange
        self.channel.queue_bind(exchange=movies_table_exchange, queue=self.queue_name)
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
            print(f"Received movies table: {self.movies_table}")
            # Notify that the movies table is ready  
            self.movies_table_ready.set()
            
            if movies_table["last"]:
                self.channel.stop_consuming()
                
        # Start consuming messages from the queue
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
        
        self.channel.start_consuming()

    def receive_batch(self):
        raise NotImplementedError("Subclasses should implement this method.")
        