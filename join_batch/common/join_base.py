import pika
import multiprocessing
import signal

class JoinBatchBase:
    def __init__(self, config):
        self.config = config
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        self.channel = self.connection.channel()
        # TODO: Sync mechanism to store movies table once received
        self.movies_table = None

        # Register signal handler for SIGTERM signal
        signal.signal(signal.SIGTERM, self.__handleSigterm)

    def __handleSigterm(self, signum, frame):
        print("SIGTERM signal received. Closing connection...")
        self.connection.close()

    def process(self):
        # Start the process to receive the movies table
        process = multiprocessing.Process(target=self.receive_movies_table)
        process.start()

        # Start the loop to receive the batches
        self.receive_batch()        

        process.join()
        

    def receive_movies_table(self):
        # Declare a fanout exchange
        self.channel.exchange_declare(exchange='broadcast', exchange_type='fanout')

        # Create a new queue with a random name
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        # Bind the queue to the exchange
        self.channel.queue_bind(exchange='broadcast', queue=queue_name)

        # Callback function to handle incoming messages
        def callback(ch, method, properties, body):
            # TODO: Safely store table
            self.movies_table = body           

        # Start consuming messages from the queue
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        
        self.channel.start_consuming()

    def receive_batch(self):
        pass
        