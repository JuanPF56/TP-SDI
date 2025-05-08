import json
import logging
from typing import Dict, Any
from common.client_state import ClientState
from common.client_state_manager import ClientManager
from common.mom import RabbitMQProcessor

EOS_TYPE = "EOS"

logger = logging.getLogger("EOS-Handling")

"""
This module handles the End of Stream (EOS) messages in a RabbitMQ-based system.
It provides functions to mark the EOS for a given input queue, check if all nodes 
have sent EOS, and propagate the EOS to output queues and exchanges.
"""

def mark_eos_received(body, input_queue, source_queues, headers,
                      nodes_of_type, rabbitmq_processor: RabbitMQProcessor, 
                      client_state: ClientState, client_manager: ClientManager,
                      target_queues=None, target_exchanges=None):
    """
    Mark the end of stream (EOS) for the given input queue
    for the given node.
    """
    try:
        data = json.loads(body)
        node_id = data.get("node_id")
        count = data.get("count", 0)
        logger.debug(f"Count received: {count}")
    except json.JSONDecodeError:
        logger.error("Failed to decode EOS message")
        return

    if not client_state.has_queue_received_eos_from_node(input_queue, node_id):
        count += 1
        logger.debug("COUNT INCREMENTED " + str(count))
        logger.debug(f"EOS count for node {node_id}: {count}")
        client_state.mark_eos(input_queue, node_id)
        check_eos_flags(headers, node_id, source_queues, rabbitmq_processor, 
                        client_state, client_manager, target_queues, target_exchanges)

    logger.debug(f"EOS received for node {node_id} from input queue {input_queue}")
    logger.debug(f"Count of EOS: {count} < {nodes_of_type}")
    # If this isn't the last node, send the EOS message back to the input queue
    if count < nodes_of_type: 
        # Send EOS back to input queue for other year nodes
        rabbitmq_processor.publish(
            target=input_queue,
            message={"node_id": node_id, "count": count},
            msg_type=EOS_TYPE,
            headers=headers
        )

def check_eos_flags(headers, node_id, source_queues, rabbitmq_processor, 
                    client_state: ClientState, client_manager: ClientManager,
                    target_queues=None, target_exchanges=None):
    """
    Check if all nodes have sent EOS and propagate to output queues.
    """
    if client_state.has_received_all_eos(source_queues):
        logger.info("All nodes have sent EOS. Sending EOS to output queues.")
        send_eos(headers, node_id, rabbitmq_processor, target_queues, target_exchanges)
        client_manager.remove_client(client_state.client_id, client_state.request_id)
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





