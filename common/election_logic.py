from common.logger import get_logger
from common.mom import RabbitMQProcessor
from common.master import MasterLogic

REC_TYPE = "RECOVERY"

logger = get_logger("ElectionLogic")

def election_logic(self, first_run: bool, leader_id: int,
                   master_logic: MasterLogic,
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
    if leader_id == self.node_id:
        logger.info("This node is the new leader.")
        master_logic.leader.set()
    elif master_logic.is_leader() and self.node_id != leader_id:
        logger.info("This node is not the leader anymore.")
        master_logic.leader.clear()
    
    logger.info("Recovery process started.")
    if master_logic.is_leader():
        logger.info(f"[Node {self.node_id}] I am the leader. Reading from storage...")
        read_storage()
    elif first_run:
        first_run = False
        logger.info(f"[Node {self.node_id}] Asking leader {leader_id} for recovery...")
        leader_queue = self.clean_batch_queue
        rabbitmq_processor.publish(
            target=leader_queue,
            message={"node_id": self.node_id},
            msg_type=REC_TYPE,
        )