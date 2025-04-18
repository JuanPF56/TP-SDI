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
        
        protocol.send_dataset(datasets_path, "movies_metadata", "BATCH_MOVIES")
        receive_confirmation = protocol.receive_confirmation()
        if receive_confirmation != ACK:
            logger.error("Server returned an error after sending movies.")
            raise Exception("Server returned an error after sending movies.")
        logger.info("Movies were sent successfully.")
        """

        protocol.send_dataset(datasets_path, "credits", "BATCH_CREDITS")
        receive_confirmation = protocol.receive_confirmation()
        if receive_confirmation != ACK:
            logger.error("Server returned an error after sending actors.")
            raise Exception("Server returned an error after sending actors.")
        logger.info("Actors were sent successfully.")
        
        
        protocol.send_dataset(datasets_path, "ratings", "BATCH_RATINGS")
        receive_confirmation = protocol.receive_confirmation()
        if receive_confirmation != ACK:
            logger.error("Server returned an error after sending ratings.")
            raise Exception("Server returned an error after sending ratings.")
        logger.info("Ratings were sent successfully.")
        """
        
        logger.info("All datasets were sent.")
