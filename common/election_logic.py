from common.logger import get_logger
from common.mom import MAX_PRIORITY, RabbitMQProcessor
from common.master import MasterLogic

REC_TYPE = "RECOVERY"

logger = get_logger("ElectionLogic")

def election_logic(first_run: bool, leader_id: int,
                    node_id: int, leader_queues: str, master_logic: MasterLogic,
                    rabbitmq_config: dict,
                    read_storage: callable = None):
    """
    Logic to be executed when a leader is elected.
    This method will be called by the LeaderElector when a new leader is elected.

    Parameters:
    - first_run: Indicates if this is the first run of the node.
    - leader_id: The ID of the newly elected leader.
    - master_logic: The master logic instance to handle the election.
    - rabbitmq_config: Configuration for RabbitMQ connection.
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
        if not isinstance(leader_queues, list):
            leader_queues = [leader_queues]
        rabbit = RabbitMQProcessor(
            config=rabbitmq_config,
            source_queues=leader_queues,
            target_queues=[queue + "_node_" + str(node_id) for queue in leader_queues]
        )
        if not rabbit.connect():
            logger.error("Error connecting to RabbitMQ. Exiting...")
            return
        logger.info(f"[Node {node_id}] Asking leader {leader_id} for recovery...")
        for queue in leader_queues:
            rabbit.publish(
                target=queue,
                message={"node_id": node_id},
                msg_type=REC_TYPE,
                priority=MAX_PRIORITY
            )
        rabbit.close()