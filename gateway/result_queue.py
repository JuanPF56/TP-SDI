import time
import os
import pickle
import threading
import queue
import uuid
from typing import Optional, Tuple, Any
from common.logger import get_logger

logger = get_logger("ResultQueue")


class ResultQueue:
    """
    Persistent queue for storing results that need to be sent to clients.
    Similar to MessageQueue but specialized for outgoing results.
    """

    def __init__(self, queue_dir: str):
        self.queue_dir = queue_dir
        self._ensure_directory()

        # In-memory queue for fast access
        self._memory_queue = queue.Queue()
        self._lock = threading.Lock()

        # Load existing messages from disk on startup
        self._load_from_disk()

    def _ensure_directory(self):
        """Ensure the queue directory exists"""
        os.makedirs(self.queue_dir, exist_ok=True)

    def _get_file_path(self, result_id: str) -> str:
        """Get the file path for a specific result"""
        return os.path.join(self.queue_dir, f"{result_id}.pkl")

    def _load_from_disk(self):
        """Load all persisted results from disk into memory queue"""
        try:
            files = [f for f in os.listdir(self.queue_dir) if f.endswith(".pkl")]
            files.sort()  # Process in order

            for filename in files:
                result_id = filename[:-4]  # Remove .pkl extension
                file_path = os.path.join(self.queue_dir, filename)

                try:
                    with open(file_path, "rb") as f:
                        result_data = pickle.load(f)

                    # Put back in memory queue
                    self._memory_queue.put((result_id, result_data))
                    logger.debug(f"Loaded result {result_id} from disk")

                except Exception as e:
                    logger.error(f"Error loading result {result_id} from disk: {e}")
                    # Remove corrupted file
                    try:
                        os.remove(file_path)
                    except:
                        pass

            if files:
                logger.info(f"Loaded {len(files)} persisted results from disk")

        except Exception as e:
            logger.error(f"Error loading results from disk: {e}")

    def put(self, client_id: str, result_data: Any) -> str:
        """
        Add a result to the queue and persist it to disk.
        Returns the unique result ID.
        """
        result_id = str(uuid.uuid4())

        # Create result entry
        result_entry = {
            "client_id": client_id,
            "result_data": result_data,
            "timestamp": time.time(),
        }

        try:
            # Persist to disk first
            file_path = self._get_file_path(result_id)
            with open(file_path, "wb") as f:
                pickle.dump(result_entry, f)

            # Then add to memory queue
            with self._lock:
                self._memory_queue.put((result_id, result_entry))

            logger.debug(f"Queued result {result_id} for client {client_id}")
            return result_id

        except Exception as e:
            logger.error(f"Error queuing result for client {client_id}: {e}")
            raise

    def get(self, timeout: Optional[float] = None) -> Tuple[str, dict]:
        """
        Get the next result from the queue.
        Returns (result_id, result_entry) or raises queue.Empty if timeout expires.
        """
        return self._memory_queue.get(timeout=timeout)

    def task_done(self, result_id: str):
        """
        Mark a result as successfully processed and remove it from disk.
        """
        try:
            file_path = self._get_file_path(result_id)
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Removed processed result {result_id} from disk")
        except Exception as e:
            logger.error(f"Error removing result {result_id} from disk: {e}")

        # Mark task as done in memory queue
        self._memory_queue.task_done()

    def qsize(self) -> int:
        """Get the approximate size of the queue"""
        return self._memory_queue.qsize()

    def empty(self) -> bool:
        """Check if the queue is empty"""
        return self._memory_queue.empty()
