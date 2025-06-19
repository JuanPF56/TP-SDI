from common.logger import get_logger
from common.mom import MAX_PRIORITY, RabbitMQProcessor
from common.master import MasterLogic

REC_TYPE = "RECOVERY"

logger = get_logger("ElectionLogic")

def election_logic(first_run: bool, leader_id: int,
                    node_id: int, leader_queue: str, master_logic: MasterLogic,
                    rabbitmq_processor: RabbitMQProcessor,
                    read_storage: callable = None):
    """
    Logic to be executed when a leader is elected.
    This method will be called by the LeaderElector when a new leader is elected.

    Parameters:
    - first_run: Indicates if this is the first run of the node.
    - leader_id: The ID of the newly elected leader.
    - master_logic: The master logic instance to handle the election.
    - rabbitmq_processor: The RabbitMQ processor instance to handle message processing.
    - read_storage: Optional callable to read from storage if this node is the leader.
    """
    logger.info(f"New leader elected: {leader_id}")
    is_now_leader = (leader_id == node_id)
    was_leader = master_logic.is_leader()
    if is_now_leader != was_leader:
        master_logic.toggle_leader()
    
    logger.info("Recovery process started.")
    if not was_leader and is_now_leader:
        logger.info(f"[Node {node_id}] I am the leader. Reading from storage...")
        read_storage()
    elif first_run:
        first_run = False
        logger.info(f"[Node {node_id}] Asking leader {leader_id} for recovery...")
        rabbitmq_processor.publish(
            target=leader_queue,
            message={"node_id": node_id},
            msg_type=REC_TYPE,
            priority=MAX_PRIORITY
        )