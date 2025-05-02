# common/client_state.py

class ClientState:
    def __init__(self, client_id):
        self.client_id = client_id
        self.batches = [] 
        self.eos_flags = {}  # key: queue_name, value: bool

    def add_batch(self, data):
        self.batches.extend(data)

    def mark_eos(self, queue_name):
        self.eos_flags[queue_name] = True

    def has_received_eos(self, queue_name):
        return self.eos_flags.get(queue_name, False)

    def all_queues_eos(self, expected_queues):
        return all(self.eos_flags.get(q, False) for q in expected_queues)
