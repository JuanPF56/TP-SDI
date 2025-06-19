from common.logger import get_logger
from common.mom import MAX_PRIORITY, RabbitMQProcessor
from common.master import MasterLogic

REC_TYPE = "RECOVERY"

logger = get_logger("ElectionLogic")

def election_logic(self, leader_id: int, leader_queues=None):
    """
    Logic to be executed when a leader is elected.
    This method will be called by the LeaderElector when a new leader is elected.

    Parameters:
    - leader_id: The ID of the newly elected leader.
    """
    if self.master_logic_started_event:
        self.master_logic_started_event.wait()
    logger.info(f"New leader elected: {leader_id}")
    is_now_leader = (leader_id == self.node_id)
    was_leader = self.master_logic.is_leader()
    if is_now_leader != was_leader:
        self.master_logic.toggle_leader()

    if not was_leader and is_now_leader:
        logger.info(f"[Node {self.node_id}] I am the leader. Reading from storage...")
        self.read_storage()
    elif self.first_run and not was_leader and not is_now_leader:
        self.first_run = False
        if not isinstance(leader_queues, list):
            leader_queues = [leader_queues]
        rabbit = RabbitMQProcessor(
            config=self.config,
            source_queues=leader_queues,
            target_queues=[queue + "_node_" + str(self.node_id) for queue in leader_queues]
        )
        if not rabbit.connect():
            logger.error("Error connecting to RabbitMQ. Exiting...")
            return
        logger.info(f"[Node {self.node_id}] Asking leader {leader_id} for recovery...")
        for queue in leader_queues:
            rabbit.publish(
                target=queue,
                message={"node_id": self.node_id},
                msg_type=REC_TYPE,
                priority=MAX_PRIORITY
            )
        rabbit.close()