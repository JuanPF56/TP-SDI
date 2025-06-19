from collections import OrderedDict

class DuplicateHandler:
    """
    Handles duplicate entries using an LRU cache for message IDs.
    LRU (Least Recently Used) cache is used to keep track of recently processed message IDs.
    """
    def __init__(self, cache_size=1000):
        self.cache = OrderedDict()
        self.cache_size = cache_size

    def add(self, message_id):
        """
        Add a message ID to the cache. If it already exists, move it to the end.
        If the cache exceeds its size limit, evict the oldest message ID.
        """
        if message_id in self.cache:
            self.cache.move_to_end(message_id)
        else:
            if len(self.cache) >= self.cache_size:
                self.cache.popitem(last=False)  # Evict the oldest
            self.cache[message_id] = None  # No value needed

    def is_duplicate(self, message_id):
        """
        Check if the message ID is a duplicate.
        """
        return message_id in self.cache
