"""This module defines the BatchMessage class used for handling batch messages in the gateway."""


class BatchMessage:
    def __init__(
        self, message_code, client_id, current_batch, is_last_batch, processed_data
    ):
        self.message_code = message_code
        self.client_id = client_id
        self.current_batch = current_batch
        self.is_last_batch = is_last_batch
        self.processed_data = processed_data
