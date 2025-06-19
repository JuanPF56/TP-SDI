from collections import OrderedDict

class DuplicateHandler:
    """
    Handles duplicate message detection per client and per queue using an LRU cache.
    LRU (Least Recently Used) cache keeps track of the most recent messages processed for each client and queue.
    Structure:
        self.cache[client_id][queue_name] = OrderedDict of message_ids
    """
    def __init__(self, cache_size=1000):
        self.cache_size = cache_size
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

    def is_duplicate(self, client_id, queue_name, message_id):
        """
        Check if a message ID is a duplicate for a specific client and queue.
        """
        return (
            client_id in self.cache and
            queue_name in self.cache[client_id] and
            message_id in self.cache[client_id][queue_name]
        )