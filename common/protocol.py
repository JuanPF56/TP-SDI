SIZE_OF_UINT8 = 1
SIZE_OF_UINT16 = 2
SIZE_OF_UINT32 = 4

"""
 Protocol:
    Header:
        1 byte: tipo_de_mensaje
        2 bytes: total_de_batches
        2 bytes: nro_batch_actual
        4 bytes: payload_len
    Payload:
        payload_len bytes: data
 """

TIPO_MENSAJE = {
    "BATCH_MOVIES": 1,
    "BATCH_ACTORS": 2,
    "BATCH_RATINGS": 3,
}

SIZE_OF_HEADER = 1 + 2 + 2 + 4  # tipo_de_mensaje (1 byte) + total_de_batches (2 bytes) + nro_batch_actual (2 bytes) + payload_len (4 bytes)

ACK = 0
SUCCESS = 0
ERROR = 1