"""
Utility functions for the client to handle dataset downloading and sending to the server.
"""

from protocol_client_gateway import ProtocolClient, ServerNotConnectedError

import kagglehub

from common.logger import get_logger

logger = get_logger("Client")


def download_dataset():
    """
    Downloads the dataset from Kaggle using kagglehub.
    Returns:
        str: The path where the dataset is downloaded.
    """
    try:
        logger.info("Downloading dataset with kagglehub...")
        path = kagglehub.dataset_download("rounakbanik/the-movies-dataset")
        logger.info("Dataset downloaded at: %s", path)
        return path
    except Exception as e:
        logger.error("Failed to download dataset: %s", e)
        return None


def send_datasets_to_server(datasets_path: str, protocol: ProtocolClient):
    """
    Sends the datasets to the server using the provided protocol.
    Args:
        datasets_path (str): The path where the datasets are located.
        protocol (ProtocolClient): The protocol client to use for sending data.
    """
    logger.info("Sending datasets to server...")
    datasets = [
        ("movies_metadata", "BATCH_MOVIES", "movies"),
        ("credits", "BATCH_CREDITS", "actors"),
        ("ratings", "BATCH_RATINGS", "ratings"),
    ]
    for filename, tag, description in datasets:
        try:
            logger.info("Sending %s dataset...", description)
            protocol.send_dataset(datasets_path, filename, tag)
            logger.info("%s dataset sent successfully.", description.capitalize())

        except ServerNotConnectedError:
            logger.error("Connection closed by server")
            raise

        except Exception as e:
            logger.error("Failed to send %s dataset: %s", description, e)
            raise

    logger.info("All datasets were sent successfully.")
