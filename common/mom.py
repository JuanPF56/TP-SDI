import pika
import json
import time

from common.logger import get_logger
logger = get_logger("RabbitMQProcessor")

RETRIES = 10
RETRY_DELAY = 3  # seconds
HEARTBEAT = 600
CONNECTION_TIMEOUT = 300

class RabbitMQProcessor:
    def __init__(self, config, source_queues, target_queues, rabbitmq_host="rabbitmq"):
        self.config = config
        self.source_queues = source_queues if isinstance(source_queues, list) else [source_queues]
        self.target_queues = target_queues 
        self.rabbitmq_host = rabbitmq_host
        self.connection = None
        self.channel = None

    def connect(self):
        """
        Stablishes a connection to RabbitMQ with retry logic.
        Retries the connection up to a specified number of times with exponential backoff.
        If the connection fails after all retries, it logs an error and returns False.
        """
        delay = RETRY_DELAY
        for attempt in range(1, RETRIES + 1):
            try:
                logger.info(f"Connecting to RabbitMQ in {self.rabbitmq_host} (attempt {attempt}/{RETRIES})")
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    heartbeat=HEARTBEAT,
                    blocked_connection_timeout=CONNECTION_TIMEOUT
                ))
                self.channel = self.connection.channel()

                logger.info("Successfully connected to RabbitMQ.")

                for queue in self.source_queues:
                    self.channel.queue_declare(queue=queue)

                # Handle target_queues being a single item, list, or dict
                if isinstance(self.target_queues, str):
                    self.channel.queue_declare(queue=self.target_queues)
                elif isinstance(self.target_queues, list):
                    for queue in self.target_queues:
                        self.channel.queue_declare(queue=queue)
                elif isinstance(self.target_queues, dict):
                    for target in self.target_queues.values():
                        if isinstance(target, list):
                            for queue in target:
                                self.channel.queue_declare(queue=queue)
                        else:
                            self.channel.queue_declare(queue=target)

                return True

            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"RabbitMQ connection failed (attempt {attempt}/{RETRIES}): {e}")
                time.sleep(delay)
                delay *= 2  # Exponential backoff

            except Exception as e:
                logger.error(f"Unexpected error while connecting to RabbitMQ: {e}")
                return False

        logger.error(f"Failed to connect to RabbitMQ after {RETRIES} attempts.")
        return False

    def consume(self, callback):
        """
        Inicia el consumo de mensajes desde las colas.
        """
        logger.info("Iniciando el consumo de mensajes...")
        for queue in self.source_queues:
            # Create a closure that properly captures the queue variable
            def create_callback_wrapper(queue_name):
                def wrapped_callback(ch, method, properties, body):
                    callback(ch, method, properties, body, queue_name)
                return wrapped_callback
            
            # Use the wrapper factory to create a callback with the correct queue captured
            self.channel.basic_consume(
                queue=queue,
                on_message_callback=create_callback_wrapper(queue),
                auto_ack=False
            )

        logger.info("Esperando mensajes...")
        self.channel.start_consuming()
        
    def publish(self, queue, message, msg_type=None):
        """
        Publica un mensaje en la cola especificada.
        """
        logger.debug(f"Publicando mensaje en la cola: {queue}, tipo: {msg_type}", extra={"payload": message})
        self.channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(type=msg_type)
        )
        logger.debug(f"Mensaje enviado a la cola: {queue}, tipo: {msg_type}")

    def stop_consuming(self):
        """
        Detiene el consumo de mensajes.
        """
        if self.channel:
            self.channel.stop_consuming()
            logger.info("Detenido el consumo de mensajes.")

    def acknowledge(self, method):
        """
        Reconoce el mensaje procesado.
        """
        if method:
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.debug("Mensaje reconocido.")
        else:
            logger.warning("No se pudo reconocer el mensaje, método no válido.")

    def close(self):
        """
        Cierra la conexión de RabbitMQ.
        """
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info("Conexión cerrada con RabbitMQ.")
