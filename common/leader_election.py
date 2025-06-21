import logging
import socket
import threading
import time
import random

from common.logger import get_logger

# logging.basicConfig(level=logging.INFO)
logger = get_logger("Leader_election")


class LeaderElector:
    def __init__(
        self, node_id, peers, election_port=9000, election_logic: callable = None
    ):
        """
        node_id: int, unique id of this node (higher wins)
        peers: dict of {node_id: (ip, port)} for all other nodes
        election_port: port number to listen/send election messages
        """
        self.node_id = node_id
        self.peers = self._parse_peers(peers)
        self.election_port = election_port
        self.election_logic = election_logic

        self.election_in_progress = False
        self.highest_seen_id = (
            self.node_id
        )  # track highest ID seen in election messages
        self.leader_id = None
        self.alive_received = False  # Track if we received ALIVE responses
        self.coordinator_announced = False  # Prevent duplicate announcements

        # Heartbeat mechanism
        self.heartbeat_interval = 3  # Send heartbeat every 3 seconds
        self.heartbeat_timeout = 10  # Consider node dead after 10 seconds
        self.last_heartbeat_received = {}  # Track last heartbeat from each peer
        self.heartbeat_running = False

        # Track if we've attempted initial election
        self.initial_election_attempted = False

        # Track failed nodes that we've detected
        self.failed_nodes = set()  # Keep track of nodes we've detected as failed

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.election_port))

        self.listener_thread = threading.Thread(target=self.listen)
        self.listener_thread.daemon = True
        self.listener_thread.start()

        # Initialize heartbeat tracking for all peers
        current_time = time.time()
        for peer_id in self.peers.keys():
            self.last_heartbeat_received[peer_id] = current_time

        # Start heartbeat mechanism first
        self.start_heartbeat()

        # Add startup delay to prevent simultaneous elections, then force initial election
        startup_delay = 10
        logger.info(
            f"[Node {self.node_id}] Waiting {startup_delay:.1f}s before starting initial election"
        )

        def delayed_startup():
            time.sleep(startup_delay)
            if not self.initial_election_attempted:
                logger.info(
                    f"[Node {self.node_id}] Starting initial election after startup delay"
                )
                self.initial_election_attempted = True
                self.start_election()

        threading.Thread(target=delayed_startup, daemon=True).start()

    def _parse_peers(self, peers_str):
        """
        Convert peers string like "filter_cleanup_1:9001,filter_cleanup_2:9002"
        into dict {1: ("filter_cleanup_1", 9001), 2: ("filter_cleanup_2", 9002)}
        Use container names instead of 127.0.0.1 for Docker networking
        """
        peers = {}
        for item in peers_str.split(","):
            name, port = item.split(":")
            node_id = int(name.split("_")[-1])
            peers[node_id] = (name, int(port))
        return peers

    def listen(self):
        logger.info(
            f"[Node {self.node_id}] Listening for election messages on port {self.election_port}"
        )
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode()
                self.handle_message(message, addr)
            except Exception as e:
                logger.error(f"Error receiving message: {e}")

    def handle_message(self, message, addr):
        parts = message.split()
        if len(parts) < 2:
            logger.warning(f"Malformed message: {message}")
            return

        cmd = parts[0]
        sender_id = int(parts[1])

        if cmd == "ELECTION":
            self.handle_election_message(sender_id)
        elif cmd == "ALIVE":
            self.handle_alive_message(sender_id)
        elif cmd == "COORDINATOR":
            self.handle_coordinator_message(sender_id)
        elif cmd == "HEARTBEAT":
            self.handle_heartbeat_message(sender_id)
        else:
            logger.warning(f"Unknown command: {cmd}")

    def handle_election_message(self, sender_id):
        logger.info(f"[Node {self.node_id}] Received ELECTION from {sender_id}")

        # Always respond with ALIVE if sender has lower ID
        if sender_id < self.node_id:
            logger.info(f"[Node {self.node_id}] Sending ALIVE to lower ID {sender_id}")
            self.send_message("ALIVE", sender_id)

            # Only start election if we don't have a current leader and aren't already in election
            if not self.election_in_progress and self.leader_id is None:
                logger.info(
                    f"[Node {self.node_id}] Starting election due to message from lower ID"
                )
                self.start_election()
            elif self.leader_id is not None:
                logger.info(
                    f"[Node {self.node_id}] Not starting election - current leader is {self.leader_id}"
                )
            return

        # If sender has higher ID, step back and let them handle it
        if sender_id > self.node_id:
            logger.info(
                f"[Node {self.node_id}] Higher ID {sender_id} started election, stepping back"
            )
            self.election_in_progress = False
            return

        # If sender has same ID (shouldn't happen but handle gracefully)
        if sender_id == self.node_id:
            logger.warning(
                f"[Node {self.node_id}] Received ELECTION from same ID - ignoring"
            )
            return

    def handle_alive_message(self, sender_id):
        logger.info(f"[Node {self.node_id}] Received ALIVE from {sender_id}")
        # If we get ALIVE, another node with higher ID is alive, so we wait
        # Don't immediately stop election - let the timeout handle it
        self.alive_received = True
        logger.info(
            f"[Node {self.node_id}] Marking alive_received=True, waiting for higher node to become leader"
        )

    def handle_coordinator_message(self, sender_id):
        logger.info(
            f"[Node {self.node_id}] Received COORDINATOR: New leader is {sender_id}"
        )

        # Validate that the sender should actually be the leader
        if self._validate_leader(sender_id):
            old_leader = self.leader_id
            self.leader_id = sender_id
            self.election_in_progress = False
            self.highest_seen_id = max(self.highest_seen_id, sender_id)

            # If election logic is provided, call it with the new leader ID
            if self.election_logic:
                logger.info(
                    f"[Node {self.node_id}] Calling election logic with leader ID {self.leader_id}"
                )
                self.election_logic(self.leader_id)

            # Log leader change for debugging
            if old_leader != sender_id:
                logger.info(
                    f"[Node {self.node_id}] Leader changed from {old_leader} to {sender_id}"
                )
        else:
            logger.warning(
                f"[Node {self.node_id}] Rejecting invalid leader {sender_id}"
            )
            # Start a new election if we reject the leader
            if not self.election_in_progress:
                self.start_election()

    def handle_heartbeat_message(self, sender_id):
        """
        Handle heartbeat messages from other nodes.
        """
        current_time = time.time()
        self.last_heartbeat_received[sender_id] = current_time

        # If we receive a heartbeat from a node we thought was failed, remove it from failed set
        if sender_id in self.failed_nodes:
            logger.info(
                f"[Node {self.node_id}] Node {sender_id} is back online, removing from failed set"
            )
            self.failed_nodes.discard(sender_id)

        # Don't log every heartbeat to avoid spam - only log occasionally
        if int(current_time) % 10 == 0:  # Log every ~10 seconds
            logger.debug(f"[Node {self.node_id}] Heartbeat from {sender_id}")

    def start_heartbeat(self):
        """
        Start the heartbeat mechanism.
        """
        if self.heartbeat_running:
            return

        self.heartbeat_running = True
        logger.info(f"[Node {self.node_id}] Starting heartbeat mechanism")

        # Start heartbeat sender thread
        heartbeat_sender_thread = threading.Thread(target=self._heartbeat_sender)
        heartbeat_sender_thread.daemon = True
        heartbeat_sender_thread.start()

        # Start heartbeat monitor thread
        heartbeat_monitor_thread = threading.Thread(target=self._heartbeat_monitor)
        heartbeat_monitor_thread.daemon = True
        heartbeat_monitor_thread.start()

    def _heartbeat_sender(self):
        """
        Periodically send heartbeat messages to all peers.
        """
        while self.heartbeat_running:
            try:
                # Send heartbeat to all peers
                for peer_id in self.peers.keys():
                    self.send_message("HEARTBEAT", peer_id)

                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"[Node {self.node_id}] Error in heartbeat sender: {e}")
                time.sleep(1)

    def _heartbeat_monitor(self):
        """
        Monitor heartbeats from other nodes and detect failures.
        """
        while self.heartbeat_running:
            try:
                current_time = time.time()
                failed_nodes = []

                for peer_id, last_heartbeat in self.last_heartbeat_received.items():
                    time_since_heartbeat = current_time - last_heartbeat

                    if time_since_heartbeat > self.heartbeat_timeout:
                        failed_nodes.append(peer_id)

                # Handle failed nodes
                for failed_node in failed_nodes:
                    self._handle_node_failure(failed_node)
                    # Reset heartbeat timer to avoid repeated failure detection
                    self.last_heartbeat_received[failed_node] = current_time

                time.sleep(2)  # Check every 2 seconds
            except Exception as e:
                logger.error(f"[Node {self.node_id}] Error in heartbeat monitor: {e}")
                time.sleep(1)

    def _handle_node_failure(self, failed_node_id):
        """
        Handle the failure of a node.
        """
        # Only process if we haven't already marked this node as failed
        if failed_node_id in self.failed_nodes:
            return

        logger.warning(
            f"[Node {self.node_id}] Detected failure of node {failed_node_id}"
        )

        # Mark node as failed
        self.failed_nodes.add(failed_node_id)

        was_leader = (self.leader_id == failed_node_id) or (
            self.leader_id is None and failed_node_id > self.node_id
        )

        if was_leader:
            logger.warning(
                f"[Node {self.node_id}] Leader/Potential leader {failed_node_id} has failed! Starting new election"
            )
            self.leader_id = None
            self.coordinator_announced = False

            # Start election after a brief delay to avoid immediate conflicts
            def delayed_election():
                time.sleep(
                    random.uniform(0.5, 1.5)
                )  # Random delay to avoid simultaneous elections
                if self.leader_id is None and not self.election_in_progress:
                    logger.info(
                        f"[Node {self.node_id}] Starting election due to leader failure"
                    )
                    self.start_election()

            threading.Thread(target=delayed_election, daemon=True).start()
        else:
            logger.info(
                f"[Node {self.node_id}] Non-leader node {failed_node_id} failed, continuing with current leader {self.leader_id}"
            )

    def stop_heartbeat(self):
        """
        Stop the heartbeat mechanism.
        """
        logger.info(f"[Node {self.node_id}] Stopping heartbeat mechanism")
        self.heartbeat_running = False

    def _validate_leader(self, leader_id):
        """
        Validate that the proposed leader should actually be the leader.
        Returns True if valid, False otherwise.
        """
        # Check if this node has a higher ID than the proposed leader
        if self.node_id > leader_id:
            logger.warning(
                f"[Node {self.node_id}] Invalid leader {leader_id} - I have higher ID"
            )
            return False

        # Check if we know of any peers with higher IDs than the proposed leader
        # BUT only consider peers that are actually reachable and not failed
        higher_peers = []
        for nid in self.peers.keys():
            if nid > leader_id and nid not in self.failed_nodes:
                # Check if this peer is reachable (has sent heartbeat recently)
                current_time = time.time()
                last_heartbeat = self.last_heartbeat_received.get(nid, 0)
                if current_time - last_heartbeat <= self.heartbeat_timeout:
                    higher_peers.append(nid)

        if higher_peers:
            logger.warning(
                f"[Node {self.node_id}] Invalid leader {leader_id} - reachable higher IDs exist: {higher_peers}"
            )
            return False

        return True

    def send_message(self, cmd, target_id):
        if target_id not in self.peers:
            logger.warning(f"[Node {self.node_id}] No peer info for node {target_id}")
            return

        try:
            addr = self.peers[target_id]
            msg = f"{cmd} {self.node_id}".encode()

            # For heartbeats, don't do connectivity test to reduce noise
            if cmd != "HEARTBEAT":
                # Test connectivity before sending
                test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                test_sock.settimeout(1)  # 1 second timeout

                try:
                    # Try to connect to test connectivity
                    test_sock.connect(addr)
                    logger.info(
                        f"[Node {self.node_id}] Network test OK for {target_id} at {addr}"
                    )
                except Exception as conn_e:
                    logger.error(
                        f"[Node {self.node_id}] Network connectivity test FAILED for {target_id} at {addr}: {conn_e}"
                    )
                finally:
                    test_sock.close()

            # Send the actual message
            self.sock.sendto(msg, addr)
            if cmd != "HEARTBEAT":  # Don't log every heartbeat
                logger.info(
                    f"[Node {self.node_id}] Sent {cmd} to {target_id} at {addr}"
                )

        except Exception as e:
            if cmd != "HEARTBEAT":  # Don't log heartbeat failures as errors
                logger.error(
                    f"[Node {self.node_id}] Failed to send {cmd} to {target_id}: {e}"
                )

    def start_election(self):
        # Prevent starting election if we already have a stable leader
        if self.leader_id is not None and not self._should_challenge_leader():
            logger.info(
                f"[Node {self.node_id}] Not starting election - stable leader {self.leader_id} exists"
            )
            return

        logger.info(f"[Node {self.node_id}] Starting election")
        self.election_in_progress = True
        self.alive_received = False
        self.highest_seen_id = self.node_id
        self.coordinator_announced = False

        # FIXED: Only consider reachable higher peers (exclude failed nodes)
        higher_peers = []
        current_time = time.time()
        for nid in self.peers.keys():
            if nid > self.node_id and nid not in self.failed_nodes:
                last_heartbeat = self.last_heartbeat_received.get(nid, 0)
                # Consider peer reachable if we've heard from them recently
                # OR if this is the very first election attempt (not subsequent ones)
                if current_time - last_heartbeat <= self.heartbeat_timeout:
                    higher_peers.append(nid)
                elif not self.initial_election_attempted:
                    # Only during initial startup, assume peers might be reachable
                    higher_peers.append(nid)

        if not higher_peers:
            # I have highest ID among reachable peers, validate before declaring leadership
            if self._can_be_leader():
                logger.info(
                    f"[Node {self.node_id}] No reachable higher peers found, declaring leadership"
                )
                self.announce_coordinator()
            else:
                logger.info(f"[Node {self.node_id}] Cannot be leader, waiting...")
                self.election_in_progress = False
            return

        # Send ELECTION to all reachable peers with higher ID
        logger.info(
            f"[Node {self.node_id}] Sending ELECTION to reachable higher peers: {higher_peers}"
        )
        for peer_id in higher_peers:
            self.send_message("ELECTION", peer_id)

        # Wait for responses for a timeout period
        def election_timeout():
            time.sleep(5)  # 5 second timeout for ALIVE
            if self.election_in_progress and not self.alive_received:
                # No ALIVE received from higher ID nodes
                if self._can_be_leader():
                    logger.info(
                        f"[Node {self.node_id}] Timeout reached, no ALIVE responses, declaring leadership"
                    )
                    self.announce_coordinator()
                else:
                    logger.info(
                        f"[Node {self.node_id}] Timeout reached but cannot be leader"
                    )
                    self.election_in_progress = False

        threading.Thread(target=election_timeout, daemon=True).start()

    def _should_challenge_leader(self):
        """
        Determine if current leader should be challenged.
        Only challenge if we have higher ID than current leader.
        """
        return self.leader_id is not None and self.node_id > self.leader_id

    def _can_be_leader(self):
        """
        Check if this node can legitimately become the leader.
        This adds an extra validation step before announcing leadership.
        """
        # FIXED: Only check reachable peers with higher IDs (exclude failed nodes)
        current_time = time.time()
        higher_reachable_peers = []

        for nid in self.peers.keys():
            if nid > self.node_id and nid not in self.failed_nodes:
                last_heartbeat = self.last_heartbeat_received.get(nid, 0)
                if current_time - last_heartbeat <= self.heartbeat_timeout:
                    higher_reachable_peers.append(nid)

        if higher_reachable_peers:
            logger.info(
                f"[Node {self.node_id}] Cannot be leader - reachable higher peers exist: {higher_reachable_peers}"
            )
            return False

        # Additional check: ensure we're not conflicting with an existing reachable leader
        if (
            self.leader_id is not None
            and self.leader_id > self.node_id
            and self.leader_id not in self.failed_nodes
        ):
            # Check if current leader is still reachable
            last_heartbeat = self.last_heartbeat_received.get(self.leader_id, 0)
            if current_time - last_heartbeat <= self.heartbeat_timeout:
                logger.info(
                    f"[Node {self.node_id}] Cannot be leader - current leader {self.leader_id} is still reachable"
                )
                return False

        return True

    def announce_coordinator(self):
        if not self._can_be_leader():
            logger.warning(
                f"[Node {self.node_id}] Aborting leadership announcement - validation failed"
            )
            self.election_in_progress = False
            return

        logger.info(f"[Node {self.node_id}] Announcing self as leader")
        self.leader_id = self.node_id
        self.election_in_progress = False
        self.highest_seen_id = self.node_id
        self.coordinator_announced = True
        self.election_backoff_attempts = 0

        all_peers = list(self.peers.keys())

        def broadcast_multiple_times():
            for i in range(3):  # repetir 3 veces
                for peer_id in all_peers:
                    if peer_id != self.node_id and peer_id not in self.failed_nodes:
                        self.send_message("COORDINATOR", peer_id)
                time.sleep(2)

        threading.Thread(target=broadcast_multiple_times, daemon=True).start()

        # If election logic is provided, call it with the new leader ID
        if self.election_logic:
            logger.info(
                f"[Node {self.node_id}] Calling election logic with leader ID {self.leader_id}"
            )
            self.election_logic(self.leader_id)

    def cleanup(self):
        """
        Clean up resources when shutting down.
        """
        logger.info(f"[Node {self.node_id}] Cleaning up resources")
        self.stop_heartbeat()
        try:
            self.sock.close()
        except:
            pass
