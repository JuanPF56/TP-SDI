import kagglehub

from common.logger import get_logger
logger = get_logger("Client")

from protocol_client_gateway import ProtocolClient
from common.protocol import ACK, ERROR

def download_dataset():
    try:
        logger.info("Downloading dataset with kagglehub...")
        path = kagglehub.dataset_download("rounakbanik/the-movies-dataset")
        logger.info(f"Dataset downloaded at: {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to download dataset: {e}")
        return None

def send_datasets_to_server(datasets_path: str, protocol: ProtocolClient):
    datasets = [
        ("movies_metadata", "BATCH_MOVIES", "movies"),
        ("credits", "BATCH_CREDITS", "actors"),
        ("ratings", "BATCH_RATINGS", "ratings"),
    ]

    for filename, tag, description in datasets:
        logger.info(f"Sending {description} dataset...")
        protocol.send_dataset(datasets_path, filename, tag)
        
        confirmation = protocol.receive_confirmation()
        if confirmation != ACK:
            logger.error(f"Server returned an error after sending {description}.")
            raise Exception(f"Server returned an error after sending {description}.")
        
        logger.info(f"{description.capitalize()} dataset sent successfully.")

    logger.info("All datasets were sent successfully.")
