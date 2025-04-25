import logging

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(asctime)s] [%(name)s] %(message)s', "%H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False # Prevent propagation to the root logger

    return logger