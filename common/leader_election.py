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
        
        # Leader ID, None if no leader elected yet
        self.leader_id = None
        
        # Setup UDP socket for election messages
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
                logger.info(f"[Node {self.node_id}] Received message: {message} from {addr}")
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
        
        logger.info(f"[Node {self.node_id}] Handling {cmd} from node {sender_id}")
        
        # TODO: Implement specific message handlers
        if cmd == "ELECTION":
            logger.info(f"[Node {self.node_id}] Received ELECTION from {sender_id}")
        elif cmd == "ALIVE":
            logger.info(f"[Node {self.node_id}] Received ALIVE from {sender_id}")
        elif cmd == "COORDINATOR":
            logger.info(f"[Node {self.node_id}] Received COORDINATOR from {sender_id}")
        else:
            logger.warning(f"[Node {self.node_id}] Unknown command: {cmd}")

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