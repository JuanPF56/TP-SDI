import json
from typing import Dict, Any
from common.client_state import ClientState
from common.client_state_manager import ClientManager
from common.mom import RabbitMQProcessor
from common.logger import get_logger

EOS_TYPE = "EOS"

logger = get_logger("EOS-Handling")

"""
This module handles the End of Stream (EOS) messages in a RabbitMQ-based system.
It provides functions to mark the EOS for a given input queue, check if all nodes 
have sent EOS, and propagate the EOS to output queues and exchanges.
"""

def mark_eos_received(body, node_id, input_queue, source_queues, headers,
                      nodes_of_type, rabbitmq_processor: RabbitMQProcessor, 
                      client_state: ClientState, client_manager: ClientManager,
                      target_queues=None, target_exchanges=None, extra_steps=None):
    """
    Mark the end of stream (EOS) for the given input queue
    for the given node.
    Parameters:
    - body: The message body containing the EOS information.
    - node_id: The ID of the current node.
    - input_queue: The input queue from which the EOS message was received.
    - source_queues: The list of source queues to check for EOS.
    - headers: The headers of the message.
    - nodes_of_type: The number of nodes of this type.
    - rabbitmq_processor: The RabbitMQ processor instance.
    - client_state: The client state instance.
    - client_manager: The client manager instance.
    - target_queues: The target queues to send the EOS message to.
    - target_exchanges: The target exchanges to send the EOS message to.
    - extra_steps: Any extra steps to perform after sending EOS.
    """
    try:
        data = json.loads(body)
        n_id = data.get("node_id", 1) # ID of the node that sent the EOS message
        count = data.get("count", 0) # Count of nodes that have acknowledged EOS message
        logger.debug(f"Count received: {count}")
    except json.JSONDecodeError:
        logger.error("Failed to decode EOS message")
        return

    if client_state and not client_state.has_queue_received_eos_from_node(input_queue, n_id):
        count += 1
        logger.debug("COUNT INCREMENTED " + str(count))
        logger.debug(f"EOS count for node {n_id}: {count}")
        client_state.mark_eos(input_queue, n_id)
        check_eos_flags(headers, node_id, source_queues, rabbitmq_processor, 
                        client_state, client_manager, target_queues, target_exchanges,
                        extra_steps)

    logger.info(f"EOS received for node {n_id} from input queue {input_queue}")
    logger.info(f"Count of EOS: {count} < {nodes_of_type}")
    # If this isn't the last node, send the EOS message back to the input queue
    if count < nodes_of_type: 
        # Send EOS back to input queue for other year nodes
        rabbitmq_processor.publish(
            target=input_queue,
            message={"node_id": n_id, "count": count},
            msg_type=EOS_TYPE,
            headers=headers,
            priority=1
        )

def check_eos_flags(headers, node_id, source_queues, rabbitmq_processor, 
                    client_state: ClientState, client_manager: ClientManager,
                    target_queues=None, target_exchanges=None, extra_steps=None):
    """
    Check if all nodes have sent EOS and propagate to output queues.
    """
    if client_state.has_received_all_eos(source_queues):
        logger.info("All nodes have sent EOS. Sending EOS to output queues.")
        send_eos(headers, node_id, rabbitmq_processor, target_queues, target_exchanges)
        #free_resources(client_state, client_manager, extra_steps)
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
                message={"node_id": node_id, "count": 0},
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
                message={"node_id": node_id, "count": 0},
                msg_type=EOS_TYPE,
                exchange=True,
                headers=headers,
                priority=1
            )
            logger.info(f"EOS message sent to {target_exchange}")


def free_resources(client_state: ClientState, client_manager: ClientManager, extra_steps=None):
    """
    Free resources associated with the client state.
    """
    logger.debug(f"Freeing resources for client {client_state.client_id} and request {client_state.request_id}")
    client_manager.remove_client(client_state.client_id, client_state.request_id)
    if extra_steps:
        extra_steps(client_state)


