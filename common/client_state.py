# common/client_state.py
from common.logger import get_logger

logger = get_logger("Client-State")

class ClientState:
    def __init__(self, client_id):
        self.client_id = client_id
        self.batches = [] 
        self.eos_flags = {}  # key: queue_name, value: bool

    def add_batch(self, data):
        self.batches.extend(data)

    def clear_batch(self):
        self.batches.clear()

    def mark_eos(self, queue_name):
        self.eos_flags[queue_name] = True

    def has_received_eos(self, queue_name):
        return self.eos_flags.get(queue_name, False)

    def all_queues_eos(self, expected_queues):

        return all(self.eos_flags.get(q, False) for q in expected_queues)
