import logging
import socket
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LeaderElector")

class LeaderElector:
    def __init__(self, node_id, peers, election_port=9000):
        """
        node_id: int, unique id of this node (higher wins)
        peers: dict of {node_id: (ip, port)} for all other nodes
        election_port: port number to listen/send election messages
        """
        self.node_id = node_id
        self.peers = self._parse_peers(peers)
        self.election_port = election_port
        
        # Election state
        self.leader_id = None
        self.election_in_progress = False
        self.alive_received = False  # Track if we received ALIVE responses
        
        # Setup UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.election_port))
        
        # Start listener thread
        self.listener_thread = threading.Thread(target=self.listen)
        self.listener_thread.daemon = True
        self.listener_thread.start()
        
        logger.info(f"[Node {self.node_id}] Initialized with peers: {self.peers}")

    def _parse_peers(self, peers_str):
        """
        Convert peers string like "filter_cleanup_1:9001,filter_cleanup_2:9002" 
        into dict {1: ("filter_cleanup_1", 9001), 2: ("filter_cleanup_2", 9002)}
        """
        peers = {}
        for item in peers_str.split(","):
            name, port = item.split(":")
            node_id = int(name.split("_")[-1])
            peers[node_id] = (name, int(port))
        return peers

    def listen(self):
        """Listen for incoming election messages."""
        logger.info(f"[Node {self.node_id}] Listening for messages on port {self.election_port}")
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode()
                self.handle_message(message, addr)
            except Exception as e:
                logger.error(f"[Node {self.node_id}] Error receiving message: {e}")

    def handle_message(self, message, addr):
        """Parse and handle incoming messages."""
        parts = message.split()
        if len(parts) < 2:
            logger.warning(f"[Node {self.node_id}] Malformed message: {message}")
            return

        cmd = parts[0]
        sender_id = int(parts[1])
        
        if cmd == "ELECTION":
            self.handle_election_message(sender_id)
        elif cmd == "ALIVE":
            self.handle_alive_message(sender_id)
        elif cmd == "COORDINATOR":
            self.handle_coordinator_message(sender_id)
        else:
            logger.warning(f"[Node {self.node_id}] Unknown command: {cmd}")

    def start_election(self):
        """
        Iniciar proceso de elección.
        Envía mensajes ELECTION a todos los peers con ID mayor.
        """
        logger.info(f"[Node {self.node_id}] Starting election")
        self.election_in_progress = True
        self.alive_received = False
        self.highest_seen_id = self.node_id

        # Encontrar peers con ID mayor
        higher_peers = [nid for nid in self.peers.keys() if nid > self.node_id]
        
        if not higher_peers:
            # Si no hay peers con ID mayor, soy el líder
            logger.info(f"[Node {self.node_id}] No higher peers found, I should be leader")
            return

        # Enviar ELECTION a todos los peers con ID mayor
        logger.info(f"[Node {self.node_id}] Sending ELECTION to higher peers: {higher_peers}")
        for peer_id in higher_peers:
            self.send_message("ELECTION", peer_id)

    def handle_election_message(self, sender_id):
        logger.info(f"[Node {self.node_id}] Received ELECTION from {sender_id}")

        # Siempre responder con ALIVE si el sender tiene ID menor
        if sender_id < self.node_id:
            logger.info(f"[Node {self.node_id}] Sending ALIVE to lower ID {sender_id}")
            self.send_message("ALIVE", sender_id)
            
            # Solo iniciar elección si no hay líder actual y no estamos ya en elección
            if not self.election_in_progress and self.leader_id is None:
                logger.info(f"[Node {self.node_id}] Starting election due to message from lower ID")
                self.start_election()
            return

        # Si sender tiene ID mayor, retroceder
        if sender_id > self.node_id:
            logger.info(f"[Node {self.node_id}] Higher ID {sender_id} started election, stepping back")
            self.election_in_progress = False
            return

        # Si sender tiene mismo ID (no debería pasar)
        if sender_id == self.node_id:
            logger.warning(f"[Node {self.node_id}] Received ELECTION from same ID - ignoring")
            return

    def handle_alive_message(self, sender_id):
        """Handle incoming ALIVE messages."""
        logger.info(f"[Node {self.node_id}] Received ALIVE from {sender_id}")
        
        # If we get ALIVE, another node with higher ID is alive, so we wait
        # This means our election attempt should wait for them to become leader
        self.alive_received = True
        logger.info(f"[Node {self.node_id}] Marking alive_received=True, waiting for higher node to become leader")

    def handle_coordinator_message(self, sender_id):
        """Handle incoming COORDINATOR messages - accept new leader."""
        logger.info(f"[Node {self.node_id}] Received COORDINATOR: New leader is {sender_id}")
        
        # Basic validation: accept if sender has higher or equal ID than us
        # (Equal ID case shouldn't happen in normal operation)
        if sender_id >= self.node_id:
            old_leader = self.leader_id
            self.leader_id = sender_id
            self.election_in_progress = False
            self.alive_received = False  # Reset for next election
            
            # Log leader change for debugging
            if old_leader != sender_id:
                logger.info(f"[Node {self.node_id}] Leader changed from {old_leader} to {sender_id}")
        else:
            logger.warning(f"[Node {self.node_id}] Rejecting invalid leader {sender_id} (lower than my ID)")

    def send_message(self, cmd, target_id):
        """Send a message to a specific node."""
        if target_id not in self.peers:
            logger.warning(f"[Node {self.node_id}] No peer info for node {target_id}")
            return False
        
        try:
            addr = self.peers[target_id]
            msg = f"{cmd} {self.node_id}".encode()
            
            self.sock.sendto(msg, addr)
            logger.info(f"[Node {self.node_id}] Sent {cmd} to node {target_id} at {addr}")
            return True
            
        except Exception as e:
            logger.error(f"[Node {self.node_id}] Failed to send {cmd} to {target_id}: {e}")
            return False
        
