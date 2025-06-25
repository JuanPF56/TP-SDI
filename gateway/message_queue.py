import time
import queue
import threading
from pathlib import Path
import pickle

from raw_message import RawMessage

from common.logger import get_logger

logger = get_logger("MessageQueue")


class MessageQueue:
    """Thread-safe message queue with disk persistence"""

    def __init__(self, persistence_dir="/tmp/gateway_messages"):
        self.queue = queue.Queue()
        self.persistence_dir = Path(persistence_dir)
        self.persistence_dir.mkdir(exist_ok=True)
        self._lock = threading.Lock()
        self._message_counter = 0

        # Restore messages from disk on startup
        self._restore_messages_from_disk()

    def put(self, raw_message: RawMessage) -> str:
        """Put message in queue and persist to disk. Returns message file ID."""
        with self._lock:
            self._message_counter += 1
            message_id = f"msg_{int(time.time() * 1000)}_{self._message_counter}"

            # Save to disk first
            self._save_message_to_disk(message_id, raw_message)

            # Then add to queue
            self.queue.put((message_id, raw_message))

            logger.debug("Message %s queued and persisted", message_id)
            return message_id

    def get(self, timeout=None):
        """Get message from queue. Returns (message_id, raw_message) tuple."""
        return self.queue.get(timeout=timeout)

    def task_done(self, message_id: str):
        """Mark task as done and remove from disk"""
        self.queue.task_done()
        self._remove_message_from_disk(message_id)
        logger.debug("Message %s processed and removed from disk", message_id)

    def _save_message_to_disk(self, message_id: str, raw_message: RawMessage):
        """Save message to disk for crash recovery"""
        try:
            message_file = self.persistence_dir / f"{message_id}.pkl"
            with open(message_file, "wb") as f:
                pickle.dump(raw_message, f)
        except Exception as e:
            logger.error("Failed to save message %s to disk: %s", message_id, e)

    def _remove_message_from_disk(self, message_id: str):
        """Remove processed message from disk"""
        try:
            message_file = self.persistence_dir / f"{message_id}.pkl"
            if message_file.exists():
                message_file.unlink()
        except Exception as e:
            logger.warning("Failed to remove message %s from disk: %s", message_id, e)

    def _restore_messages_from_disk(self):
        """Restore unprocessed messages from disk on startup"""
        try:
            message_files = list(self.persistence_dir.glob("msg_*.pkl"))
            if not message_files:
                logger.info("No unprocessed messages found on disk")
                return

            logger.info(
                "Restoring %d unprocessed messages from disk", len(message_files)
            )

            # Sort by timestamp to maintain order
            message_files.sort(key=lambda x: x.stem)

            for message_file in message_files:
                try:
                    with open(message_file, "rb") as f:
                        raw_message = pickle.load(f)

                    message_id = message_file.stem
                    self.queue.put((message_id, raw_message))
                    logger.debug("Restored message %s from disk", message_id)

                except Exception as e:
                    logger.error("Failed to restore message %s: %s", message_file, e)
                    # Remove corrupted file
                    try:
                        message_file.unlink()
                    except:
                        pass

        except Exception as e:
            logger.error("Error during message restoration: %s", e)

    def qsize(self):
        """Return approximate queue size"""
        return self.queue.qsize()
