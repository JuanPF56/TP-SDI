"""
Common Message Oriented Middleware (MOM) module for RabbitMQ processing.
"""

import time
import json
import pika

from common.logger import get_logger

logger = get_logger("RabbitMQProcessor")

RETRIES = 10
RETRY_DELAY = 3  # seconds
HEARTBEAT = 600
CONNECTION_TIMEOUT = 300

MAX_PRIORITY = 10


class RabbitMQProcessor:
    def __init__(
        self,
        config,
        source_queues,
        target_queues,
        rabbitmq_host="rabbitmq",
        source_exchange=None,
        target_exchange=None,
    ):
        """
        Initializes the RabbitMQProcessor with the given configuration and queues.
        :param config: Configuration object containing RabbitMQ settings.
        :param source_queues: List of source queues to consume messages from.
        :param target_queues: List of target queues to publish messages to.
        :param rabbitmq_host: Hostname of the RabbitMQ server.
        :param source_exchange: Name of the source exchange to bind queues to.
        :param target_exchange: Name of the target exchange to publish messages to.
        """
        self.config = config
        self.source_queues = (
            source_queues if isinstance(source_queues, list) else [source_queues]
        )
        self.target_queues = target_queues
        self.source_exchange = source_exchange
        self.target_exchange = target_exchange
        self.rabbitmq_host = rabbitmq_host
        self.connection = None
        self.channel = None

    def connect(self, node_name=None):
        """
        Stablishes a connection to RabbitMQ with retry logic.
        Retries the connection up to a specified number of times with exponential backoff.
        If the connection fails after all retries, it logs an error and returns False.
        """
        delay = RETRY_DELAY
        for attempt in range(1, RETRIES + 1):
            try:
                logger.debug(
                    "Connecting to RabbitMQ in %s (attempt %d/%d)",
                    self.rabbitmq_host,
                    attempt,
                    RETRIES,
                )
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.rabbitmq_host,
                        heartbeat=HEARTBEAT,
                        blocked_connection_timeout=CONNECTION_TIMEOUT,
                    )
                )
                self.channel = self.connection.channel()

                logger.info("Successfully connected to RabbitMQ.")

                for queue in self.source_queues:
                    self.channel.queue_declare(
                        queue=queue, arguments={"x-max-priority": MAX_PRIORITY}
                    )

                # Handle target_queues being a single item, list, or dict
                if isinstance(self.target_queues, str):
                    self.channel.queue_declare(
                        queue=self.target_queues,
                        arguments={"x-max-priority": MAX_PRIORITY},
                    )
                elif isinstance(self.target_queues, list):
                    for queue in self.target_queues:
                        self.channel.queue_declare(
                            queue=queue, arguments={"x-max-priority": MAX_PRIORITY}
                        )
                elif isinstance(self.target_queues, dict):
                    for target in self.target_queues.values():
                        if isinstance(target, list):
                            for queue in target:
                                self.channel.queue_declare(
                                    queue=queue,
                                    arguments={"x-max-priority": MAX_PRIORITY},
                                )
                        else:
                            self.channel.queue_declare(
                                queue=target, arguments={"x-max-priority": MAX_PRIORITY}
                            )

                # Handle source_exchange and target_exchange if provided
                if self.source_exchange:
                    self.channel.exchange_declare(
                        exchange=self.source_exchange, exchange_type="fanout"
                    )
                    # Declare a exclusive queue for the source exchange for this node
                    if node_name is not None:
                        queue_name = f"{self.source_exchange}_node_{node_name}"
                    else:
                        queue_name = f"{self.source_exchange}_node"
                    result = self.channel.queue_declare(
                        queue=queue_name,
                        exclusive=True,
                        arguments={"x-max-priority": MAX_PRIORITY},
                    )
                    queue_name = result.method.queue
                    self.channel.queue_bind(
                        exchange=self.source_exchange, queue=queue_name
                    )
                    # Add the queue to the source_queues for consumption
                    self.source_queues.append(queue_name)

                if self.target_exchange:
                    self.channel.exchange_declare(
                        exchange=self.target_exchange, exchange_type="fanout"
                    )

                return True

            except pika.exceptions.AMQPConnectionError as e:
                logger.debug(
                    "RabbitMQ connection failed (attempt %d/%d): %s",
                    attempt,
                    RETRIES,
                    e,
                )
                time.sleep(delay)
                delay *= 2  # Exponential backoff

            except Exception as e:
                logger.error("Unexpected error while connecting to RabbitMQ: %s", e)
                return False

        logger.error("Failed to connect to RabbitMQ after %d attempts.", RETRIES)
        return False

    def reject(self, method, requeue=True):
        """Reject a message, optionally requeuing it"""
        if self.channel and not self.channel.is_closed:
            self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=requeue)

    def consume(self, callback):
        """
        Starts consuming messages from the specified source queues.
        """
        logger.info("Iniciando el consumo de mensajes...")
        self.channel.basic_qos()
        for queue in self.source_queues:
            # Create a closure that properly captures the queue variable
            def create_callback_wrapper(queue):
                def wrapped_callback(ch, method, properties, body):
                    callback(ch, method, properties, body, queue)

                return wrapped_callback

            # Use the wrapper factory to create a callback with the correct queue captured
            self.channel.basic_consume(
                queue=queue,
                on_message_callback=create_callback_wrapper(queue),
                auto_ack=False,
            )
        try:
            logger.info("Esperando mensajes...")
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("Error de conexión al consumir mensajes: %s", e)
        except pika.exceptions.ChannelClosedByBroker as e:
            logger.warning("Canal cerrado por el broker: %s", e)
        except Exception as e:
            logger.error("Error inesperado al consumir mensajes: %s", e)

    def publish(
        self,
        target,
        message,
        msg_type=None,
        exchange=False,
        headers=None,
        priority=MAX_PRIORITY - 1,
    ):
        if not self.connection or self.connection.is_closed:
            logger.warning("Publish aborted: RabbitMQ connection is closed.")
            return False

        channel = None
        success = False

        try:
            channel = self.connection.channel()

            properties = pika.BasicProperties(
                type=msg_type,
                headers=headers,
                priority=priority,
            )

            exchange_name = target if exchange else ""
            routing_key = "" if exchange else target

            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=json.dumps(message),
                properties=properties,
            )

            success = True
            logger.debug(
                "Mensaje enviado a: %s, tipo: %s, headers: %s",
                target,
                msg_type,
                headers,
            )

        except pika.exceptions.AMQPConnectionError as e:
            logger.error("Error de conexión al publicar en %s: %s", target, e)
        except pika.exceptions.ChannelClosedByBroker as e:
            logger.error("Canal cerrado por el broker al publicar en %s: %s", target, e)
        except Exception as e:
            logger.error("Error publicando mensaje a %s: %s", target, e)
        finally:
            try:
                channel.close()
            except Exception as e:
                logger.warning("No se pudo cerrar el canal correctamente: %s", e)
                return False
            return success

    def stop_consuming(self):
        """
        Detiene el consumo de mensajes.
        """
        if self.channel:
            self.channel.stop_consuming()
            logger.info("Detenido el consumo de mensajes.")

    def stop_consuming_threadsafe(self):
        if self.channel and self.channel.is_open:
            self.channel.connection.add_callback_threadsafe(self.channel.stop_consuming)

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
        if self.connection:
            if self.channel and self.channel.is_open:
                try:
                    self.channel.close()
                except Exception as e:
                    logger.warning("No se pudo cerrar el canal: %s", e)

            if self.connection.is_open:
                try:
                    self.connection.close()
                except Exception as e:
                    logger.warning("No se pudo cerrar la conexión: %s", e)

            logger.info("Conexión cerrada con RabbitMQ.")

    def reconnect_and_restart(self, callback):
        """Reconecta y reinicia el procesamiento de mensajes."""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            logger.warning("Error al cerrar la conexión: %s", e)
        self.connect()
        self.consume(callback)
