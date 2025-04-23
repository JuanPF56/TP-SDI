import json
import os
import time
import pika
import multiprocessing
import signal

EOS_TYPE = "EOS"

SECONDS_TO_HEARTBEAT = 600  # 10 minutes (adjustable)

class JoinBatchBase:
    def __init__(self, config):
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

        # Send the EOS message to the output queue
        self.log_info("Sending EOS message to output queue...")
        self.channel.basic_publish(
            exchange='',
            routing_key=self.output_queue,
            body=b'',
            properties=pika.BasicProperties(type=EOS_TYPE)
        )

        self.table_receiver.join()

    def receive_movies_table(self):
        # Get the movies table exchange name from the config
        movies_table_exchange = self.config["DEFAULT"].get("movies_table_exchange", "movies_table_broadcast")
        # Connect to RabbitMQ
        conn, chan = self.__connect_to_rabbitmq()

        def hdlSigTermTableRcv(signum, frame):
            chan.stop_consuming()
            chan.close()

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, hdlSigTermTableRcv)

        # Declare a fanout exchange
        chan.exchange_declare(exchange=movies_table_exchange, exchange_type='fanout')
        # Create a new queue with a random name
        result = chan.queue_declare(queue='', exclusive=True)
        table_queue = result.method.queue
        # Bind the queue to the exchange
        chan.queue_bind(exchange=movies_table_exchange, queue=table_queue)

        # Callback function to handle incoming messages
        def callback(ch, method, properties, body):
            try:
                movies = json.loads(body)
                self.movies_table.extend(movies)
                self.movies_table_ready.set()
                ch.stop_consuming()
            except Exception as e:
                self.log_info(f"Error receiving movies table: {e}")
        
        self.log_info("Waiting for movies table...")
        # Start consuming messages from the queue
        chan.basic_consume(queue=table_queue, on_message_callback=callback, auto_ack=True)
        
        chan.start_consuming()
        chan.close()

    def receive_batch(self):
       # Wait for the movies table to be received
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
        raise NotImplementedError("Subclasses must implement this method")
    
    def log_info(self, message):
        raise NotImplementedError("Subclasses must implement this method")