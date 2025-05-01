class ServerNotConnectedError(Exception):
    """Exception raised when the server is not connected."""
    pass

class ProtocolError(Exception):
    """Exception raised for protocol-related errors."""
    pass