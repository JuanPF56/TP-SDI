from collections import OrderedDict
import fcntl
import json
import os
from common.logger import get_logger

logger = get_logger("Duplicate-Handler")

class DuplicateHandler:
    """
    Handles duplicate message detection per client and per queue using an LRU cache.
    LRU (Least Recently Used) cache keeps track of the most recent messages processed for each client and queue.
    Structure:
        self.cache[client_id][queue_name] = OrderedDict of message_ids
    """
    def __init__(self, node_id=0, cache_size=100, no_storage=False):
        self.cache_size = cache_size
        self.node_id = node_id
        self.no_storage = no_storage
        self.cache = {}

    def add(self, client_id, queue_name, message_id):
        """
        Add a message ID to the cache for a specific client and queue.
        """
        queue_cache = self.cache.setdefault(client_id, {}).setdefault(queue_name, OrderedDict())

        if message_id in queue_cache:
            queue_cache.move_to_end(message_id)
        else:
            if len(queue_cache) >= self.cache_size:
                queue_cache.popitem(last=False)
            queue_cache[message_id] = None

        if not self.no_storage:
            self.write_storage(self.node_id)

    def is_duplicate(self, client_id, queue_name, message_id):
        """
        Check if a message ID is a duplicate for a specific client and queue.
        """
        return (
            client_id in self.cache and
            queue_name in self.cache[client_id] and
            message_id in self.cache[client_id][queue_name]
        )
    
    def get_cache(self, client_id, queue_name):
        """
        Get the cache for a specific client and queue.
        Returns an OrderedDict of message IDs.
        """
        return self.cache.get(client_id, {}).get(queue_name, OrderedDict())
    
    def get_caches(self):
        """
        Get the entire cache.
        Returns a dictionary of client_id -> queue_name -> OrderedDict of message IDs.
        """
        return self.cache
    
    def write_storage(self):
        """
        Write the current cache to a storage system.
        """
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)
            logger.info("Created storage directory: %s", storage_dir)
        storage_dir = os.path.join(storage_dir, str(self.node_id))
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)
            logger.info("Created storage directory for node %s: %s", self.node_id, storage_dir)
        
        file_path = os.path.join(storage_dir, f"duplicate_cache_{self.node_id}.json")
        tmp_file = os.path.join(storage_dir, f".tmp_duplicate_cache_{self.node_id}.json")

        try:
            with open(tmp_file, 'w') as tf:
                fcntl.flock(tf.fileno(), fcntl.LOCK_EX)
                json.dump(self.cache, tf, indent=4)
                tf.flush()
                os.fsync(tf.fileno())
            os.replace(tmp_file, file_path)
            logger.debug("Successfully wrote duplicate cache to %s", file_path)
        except Exception as e:
            logger.error("Failed to write duplicate cache: %s", e)
            if tmp_file and os.path.exists(tmp_file):
                os.remove(tmp_file)
                logger.debug("Removed temporary file: %s", tmp_file)

    def read_storage(self):
        """
        Read the cache from a storage system.
        """
        storage_dir = os.path.join(storage_dir, str(self.node_id))
        file_path = os.path.join(storage_dir, f"duplicate_cache_{self.node_id}.json")

        if not os.path.exists(file_path):
            logger.warning("Duplicate cache file does not exist: %s", file_path)
            return

        try:
            with open(file_path, 'r') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                data = json.load(f)
                self.cache = data
            logger.info("Successfully read duplicate cache from %s. Cache size: %d", file_path, len(self.cache))
        except Exception as e:
            logger.error("Failed to read duplicate cache: %s", e)
            self.cache = {}
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.error("Removed corrupted duplicate cache file: %s", file_path)

        



        