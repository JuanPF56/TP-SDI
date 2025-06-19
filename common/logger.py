import logging
import colorlog

logging.getLogger("pika").setLevel(logging.WARNING)

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()

        formatter = colorlog.ColoredFormatter(
            "%(log_color)s[%(asctime)s] [%(name)s] %(message)s",
            datefmt="%H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False  # Evita duplicados en la consola

    return logger
