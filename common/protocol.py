"""Common protocol definitions for client-server communication."""

import struct

SIZE_OF_UINT8 = 1
SIZE_OF_UINT16 = 2
SIZE_OF_UINT32 = 4

"""
 Protocol for sending batches from client to server:
    Header:
        4 bytes: message_id
        1 byte: tipo_de_mensaje
        36 bytes: UUID
        4 bytes: nro_batch_actual
        1 byte: es_el_ultimo_batch
        4 bytes: payload_len
    Payload:
        payload_len bytes: data
 """

TIPO_MENSAJE = {
    "CLIENT_ID": 0,
    "BATCH_MOVIES": 1,
    "BATCH_CREDITS": 2,
    "BATCH_RATINGS": 3,
    "RESULTS": 4,
    "DISCONNECT": 5,
}

SIZE_OF_UUID = 36  # El tamaño del UUID como string (36 caracteres)

SIZE_OF_HEADER = (
    4 + 1 + SIZE_OF_UUID + 4 + 1 + 4
)  # message_id (4 bytes) + tipo_de_mensaje (1 byte) + UUID (36 bytes) + nro_batch_actual (4 bytes) + es_el_ultimo_batch (1 byte) + payload_len (4 bytes)
HEADER_FORMAT = ">IB36sIBI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


def pack_header(
    message_id: int,
    tipo_de_mensaje: int,
    encoded_id: bytes,
    batch_number: int,
    is_last_batch: int,
    payload_length: int,
) -> bytes:
    """
    Empaqueta los valores del header en bytes usando el formato definido.
    """
    return struct.pack(
        HEADER_FORMAT,
        message_id,
        tipo_de_mensaje,
        encoded_id,
        batch_number,
        is_last_batch,
        payload_length,
    )


def unpack_header(header_bytes: bytes):
    """
    Desempaqueta un header en sus componentes.
    Retorna una tupla: (message_id, tipo_de_mensaje, encoded_id, batch_number, is_last_batch, payload_length)
    """
    if len(header_bytes) != HEADER_SIZE:
        raise ValueError(
            f"Header size mismatch: expected {HEADER_SIZE}, got {len(header_bytes)}"
        )
    return struct.unpack(HEADER_FORMAT, header_bytes)


IS_LAST_BATCH_FLAG = 1

"""
 Protocol for sending results from server to client:
    Header:
        1 byte: tipo_de_mensaje
        1 bytes: query_id
        4 bytes: payload_len
    Payload:
        payload_len bytes: data
 """
SIZE_OF_HEADER_RESULTS = (
    1 + 1 + 4
)  # tipo_de_mensaje (1 byte) + query_id (1 byte) + payload_len (4 bytes)
RESULT_HEADER_FORMAT = ">BBI"
RESULT_HEADER_SIZE = struct.calcsize(RESULT_HEADER_FORMAT)


def pack_result_header(tipo_mensaje: int, query_id: int, payload_len: int) -> bytes:
    """
    Empaqueta un header de respuesta con tipo de mensaje, query_id y longitud del payload.
    """
    return struct.pack(RESULT_HEADER_FORMAT, tipo_mensaje, query_id, payload_len)


def unpack_result_header(header_bytes: bytes):
    """
    Desempaqueta un header de respuesta.
    Retorna una tupla: (tipo_mensaje, query_id, payload_len)
    """
    if len(header_bytes) != RESULT_HEADER_SIZE:
        raise ValueError(
            f"Header size mismatch: expected {RESULT_HEADER_SIZE}, got {len(header_bytes)}"
        )
    return struct.unpack(RESULT_HEADER_FORMAT, header_bytes)


# Exceptions for protocol errors
class ProtocolError(Exception):
    """Exception raised for protocol-related errors."""

    pass
