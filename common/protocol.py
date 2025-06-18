SIZE_OF_UINT8 = 1
SIZE_OF_UINT16 = 2
SIZE_OF_UINT32 = 4

"""
 Protocol for sending batches from client to server:
    Header:
        1 byte: tipo_de_mensaje
        36 bytes: UUID
        4 bytes: nro_batch_actual
        1 byte: es_el_ultimo_batch
        4 bytes: payload_len
    Payload:
        payload_len bytes: data
 """

TIPO_MENSAJE = {
    "BATCH_MOVIES": 1,
    "BATCH_CREDITS": 2,
    "BATCH_RATINGS": 3,
    "RESULTS": 4,
    "DISCONNECT": 5,
}

SIZE_OF_UUID = 36  # El tamaño del UUID como string (36 caracteres)

SIZE_OF_HEADER = (
    1 + SIZE_OF_UUID + 4 + 1 + 4
)  # tipo_de_mensaje (1 byte) + UUID (36 bytes) + nro_batch_actual (4 bytes) + es_el_ultimo_batch (1 byte) + payload_len (4 bytes)

ACK = 0
SUCCESS = 0
ERROR = 1

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
    1 + SIZE_OF_UUID + 4 + 1 + 4
)  # tipo_de_mensaje (1 byte) + UUID (36 bytes) + nro_batch_actual_igual_a_0 (4 bytes) + query_id (1 byte) + payload_len (4 bytes)


# Exceptions for protocol errors
class ProtocolError(Exception):
    """Exception raised for protocol-related errors."""

    pass
