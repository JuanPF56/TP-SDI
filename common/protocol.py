SIZE_OF_UINT8 = 1
SIZE_OF_UINT16 = 2
SIZE_OF_UINT32 = 4

"""
 Protocol:
    Header:
        1 byte: tipo_de_mensaje
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
}

SIZE_OF_HEADER = 1 + 4 + 1 + 4  # tipo_de_mensaje (1 byte) + nro_batch_actual (4 bytes) + es_el_ultimo_batch (1 byte) + payload_len (4 bytes)

ACK = 0
SUCCESS = 0
ERROR = 1

IS_LAST_BATCH_FLAG = 1