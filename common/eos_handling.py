import json
from typing import Dict, Any
from common.client_state import ClientState
from common.mom import RabbitMQProcessor
from common.logger import get_logger

EOS_TYPE = "EOS"

logger = get_logger("EOS-Handling")

"""
This module handles the End of Stream (EOS) messages in a RabbitMQ-based system.
It provides functions to mark the EOS for a given input queue, check if all nodes 
have sent EOS, and propagate the EOS to output queues and exchanges.
"""

def handle_eos(body, node_id, input_queue, source_queues, headers,
                      rabbitmq_processor: RabbitMQProcessor, client_state: ClientState,
                      is_leader=False, target_queues=None, target_exchanges=None):
    """
    Mark the end of stream (EOS) for the given input queue for the given node 
    if it hasn't been marked yet. 
    Check if all nodes have sent EOS and propagate to output queues.

    Parameters:
    - body: The message body containing the EOS information.
    - node_id: The ID of the current node.
    - input_queue: The input queue from which the EOS message was received.
    - source_queues: The list of source queues to check for EOS.
    - headers: The headers of the message.
    - rabbitmq_processor: The RabbitMQ processor instance.
    - client_state: The client state instance.
    - target_queues: The target queues to send the EOS message to.
    - target_exchanges: The target exchanges to send the EOS message to.
    """
    try:
        data = json.loads(body)
        n_id = data.get("node_id", 1) # ID of the node that sent the EOS message
    except json.JSONDecodeError:
        logger.error("Failed to decode EOS message")
        return

    if client_state and not client_state.has_queue_received_eos_from_node(input_queue, n_id):
        client_state.mark_eos(input_queue, n_id)
        #if is_leader:
            #done_reading.wait()
            #client_state.write_storage()
        check_eos_flags(headers, node_id, source_queues, rabbitmq_processor, 
                        client_state, target_queues, target_exchanges)

    logger.info(f"EOS received for node {n_id} from input queue {input_queue}")

def check_eos_flags(headers, node_id, source_queues, rabbitmq_processor, 
                    client_state: ClientState, target_queues=None, target_exchanges=None):
    """
    Check if all nodes have sent EOS and propagate to output queues.

    Parameters:
    - headers: The headers of the message.
    - node_id: The ID of the current node.
    - source_queues: The list of source queues to check for EOS.
    - rabbitmq_processor: The RabbitMQ processor instance.
    - client_state: The client state instance.
    - target_queues: The target queues to send the EOS message to.
    - target_exchanges: The target exchanges to send the EOS message to.
    """
    if client_state.has_received_all_eos(source_queues):
        logger.info("All nodes have sent EOS. Sending EOS to output queues.")
        send_eos(headers, node_id, rabbitmq_processor, target_queues, target_exchanges)
    else:
        logger.debug("Not all nodes have sent EOS yet. Waiting...")

def send_eos(headers, node_id, rabbitmq_processor: RabbitMQProcessor, 
             target_queues=None, target_exchanges=None):
    """
    Propagate the end of stream (EOS) to all output queues and exchanges.
    """
    logger.debug("Sending EOS to output queue and exchange")
    if target_queues:
        if not isinstance(target_queues, list):
            targets = [target_queues]
        else:
            targets = target_queues
        for target_queue in targets:
            rabbitmq_processor.publish(
                target=target_queue,
                message={"node_id": node_id},
                msg_type=EOS_TYPE,
                headers=headers,
                priority=1
            )
            logger.info(f"EOS message sent to {target_queue}")
    if target_exchanges:
        if not isinstance(target_exchanges, list):
            targets = [target_exchanges]
        else:
            targets = target_exchanges
        for target_exchange in targets:
            rabbitmq_processor.publish(
                target=target_exchange,
                message={"node_id": node_id},
                msg_type=EOS_TYPE,
                exchange=True,
                headers=headers,
                priority=1
            )
            logger.info(f"EOS message sent to {target_exchange}")
        



