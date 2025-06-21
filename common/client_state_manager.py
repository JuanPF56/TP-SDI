# common/client_manager.py
import multiprocessing
import os
from common import logger
from common.client_state import ClientState
import json
import tempfile

from common.eos_handling import check_eos_flags
from common.mom import RabbitMQProcessor

logger = logger.get_logger("Client-Manager")

class ClientManager:
    def __init__(self, expected_queues, nodes_to_await=1):

        self.clients = {}  # Dictionary to hold client_id to ClientState mapping
        self.expected_queues = expected_queues
        self.nodes_to_await = nodes_to_await

    def add_client(self, client_id, is_eos=False) -> ClientState:
        """
        Add a new client or retrieve an existing one.
        """
        if client_id not in self.clients:
            if is_eos:
                return None
            self.clients[client_id] = ClientState(client_id, self.nodes_to_await)
        return self.clients[client_id]

    def remove_client(self, client_id):
        """
        Remove a client from the manager.
        """
        if client_id in self.clients:
            del self.clients[client_id]

    def get_clients(self):
        """
        Get all clients managed by this ClientManager.
        Returns a dictionary of client_id to ClientState.
        """
        return self.clients.copy()
    
    def read_storage(self):
        """
        Read the client states from storage.
        """
        storage_dir = "./storage"
        
        for filename in os.listdir(storage_dir):
            if filename.startswith("eos_") and filename.endswith(".json"):
                client_id = filename[4:-5]
                file_path = os.path.join(storage_dir, filename)
                updated = False
                try:
                    with open(file_path, "r") as f:
                        eos_flags = json.load(f)
                except Exception as e:
                    logger.error(f"Error reading client state from {file_path}: {e}")
                    continue
                
                if client_id not in self.clients:
                    self.clients[client_id] = ClientState(client_id, self.nodes_to_await)

                # Compare and update
                self.clients[client_id].eos_flags, updated = self.compare_flags(
                    eos_flags,
                    self.clients[client_id].get_eos_flags()
                )

                logger.debug("Client state read from storage for client %s", client_id)

                # If updated, write back to storage
                if updated:
                    logger.info("Client state updated for client %s", client_id)
                    logger.info("EOS flags: %s", self.clients[client_id].eos_flags)
                    tmp_file = None
                    try:
                        # Write updated data atomically
                        with tempfile.NamedTemporaryFile("w", dir=storage_dir, delete=False) as tf:
                            json.dump(self.clients[client_id].eos_flags, tf)
                            tmp_file = tf.name
                        os.replace(tmp_file, file_path)
                        logger.debug("Client state updated in storage for client %s", client_id)
                    except Exception as e:
                        logger.error(f"Error updating client state for {client_id}: {e}")
                        if tmp_file and os.path.exists(tmp_file):
                            os.remove(tmp_file)

    def compare_flags(self, new_flags, existing_flags):
        """
        Compare the new flags with the existing flags and return the updated flags.
        This is used to ensure that the client state is consistent with the storage.
        """
        updated = False
        for queue_name, nodes in new_flags.items():
            if queue_name not in existing_flags:
                existing_flags[queue_name] = {}
            if not isinstance(nodes, dict):
                logger.error(f"Invalid format for nodes in queue {queue_name}: {nodes}")
                continue
            for node_id, flag in nodes.items():
                if node_id not in existing_flags[queue_name]:
                    updated = True
                elif existing_flags[queue_name][node_id] != flag:
                    updated = True
                existing_flags[queue_name][node_id] = flag
                
        return existing_flags, updated
    
    def check_all_eos_received(self, config, node_id, queues, target_queues=None, target_exchange=None):
        """
        Check if all EOS messages have been received for a specific client and queue.
        Returns True if all EOS messages have been received, False otherwise.
        """
        rabbit = RabbitMQProcessor(
            config=config,
            source_queues=queues,
            target_queues=target_queues,
            target_exchange=target_exchange            
        )
        if not rabbit.connect():
            logger.error("Error connecting to RabbitMQ. Exiting...")
            return
        if not isinstance(queues, list):
            queues = [queues]
        for client_id, client_state in self.clients.items():
            check_eos_flags(
                headers={"client_id": client_id},
                node_id=node_id,
                source_queues=queues,
                rabbitmq_processor=rabbit,
                client_state=client_state,
                target_queues=target_queues,
                target_exchanges=target_exchange
            )
        rabbit.close()


