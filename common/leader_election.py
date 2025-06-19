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
        
        # Basic state
        self.leader_id = None
        
        # Setup UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.election_port))
        
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