"""
protocol.py - Robust message framing, serialization, and protocol definitions.

Implements length-prefixed TCP message framing (4-byte big-endian unsigned integer
indicating JSON payload length) to prevent fragmentation or concatenation issues.
"""

import json
import struct
import socket
import asyncio
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("p2p_chat.protocol")

# Maximum permitted payload size (64 KB is generous for chat and signaling metadata)
MAX_MESSAGE_SIZE = 65536
HEADER_STRUCT = struct.Struct(">I")
HEADER_SIZE = HEADER_STRUCT.size


# Signaling Server Action Types
ACTION_REGISTER = "register"
ACTION_REGISTER_ACK = "register_ack"
ACTION_REGISTER_ERROR = "register_error"
ACTION_LIST_USERS = "list_users"
ACTION_USER_LIST = "user_list"
ACTION_CONNECT_REQ = "connect_req"
ACTION_PEER_INFO = "peer_info"
ACTION_CONNECT_ERROR = "connect_error"
ACTION_USER_DISCONNECTED = "user_disconnected"
ACTION_HEARTBEAT = "heartbeat"
ACTION_HEARTBEAT_ACK = "heartbeat_ack"

# WebRTC Signaling Action Types (routed through signaling server)
ACTION_WEBRTC_OFFER = "webrtc_offer"
ACTION_WEBRTC_ANSWER = "webrtc_answer"
ACTION_WEBRTC_ICE = "webrtc_ice"

# Direct Peer-to-Peer Action Types (WebRTC DataChannel / legacy P2P)
ACTION_PEER_HANDSHAKE = "peer_handshake"
ACTION_PEER_HANDSHAKE_ACK = "peer_handshake_ack"
ACTION_CHAT_MESSAGE = "chat_message"
ACTION_PEER_DISCONNECT = "peer_disconnect"

# Explicit whitelist of actions permitted at the signaling server
ALLOWED_SIGNALING_ACTIONS = {
    ACTION_REGISTER,
    ACTION_LIST_USERS,
    ACTION_CONNECT_REQ,
    ACTION_HEARTBEAT,
    ACTION_WEBRTC_OFFER,
    ACTION_WEBRTC_ANSWER,
    ACTION_WEBRTC_ICE,
}


def send_msg(sock: socket.socket, data: Dict[str, Any]) -> None:
    """
    Serialize data dictionary to JSON, prefix with a 4-byte length header,
    and transmit over the socket using sendall.
    """
    raw_payload = json.dumps(data).encode("utf-8")
    payload_len = len(raw_payload)
    if payload_len > MAX_MESSAGE_SIZE:
        raise ValueError(f"Payload size ({payload_len} bytes) exceeds maximum limit ({MAX_MESSAGE_SIZE} bytes)")

    header = HEADER_STRUCT.pack(payload_len)
    sock.sendall(header + raw_payload)


def _recv_exact(sock: socket.socket, num_bytes: int) -> Optional[bytes]:
    """
    Read exactly num_bytes from a socket.
    Returns None if the remote end closed the connection before receiving any bytes.
    Raises ConnectionResetError if connection closes prematurely mid-message.
    """
    chunks = []
    received = 0
    while received < num_bytes:
        try:
            chunk = sock.recv(num_bytes - received)
        except (ConnectionResetError, ConnectionAbortedError, TimeoutError, OSError) as e:
            if received == 0:
                return None
            raise ConnectionResetError(f"Connection lost while reading payload ({received}/{num_bytes} bytes): {e}") from e

        if not chunk:
            # EOF reached
            if received == 0:
                return None
            raise ConnectionResetError(f"Connection closed by peer prematurely ({received}/{num_bytes} bytes received)")
        chunks.append(chunk)
        received += len(chunk)

    return b"".join(chunks)


def recv_msg(sock: socket.socket) -> Optional[Dict[str, Any]]:
    """
    Receive a length-prefixed JSON message from the socket.
    Returns the parsed dictionary, or None if the remote endpoint cleanly closed the connection.
    Raises ValueError or ConnectionResetError on malformed data or premature disconnection.
    """
    header_data = _recv_exact(sock, HEADER_SIZE)
    if header_data is None:
        return None

    (payload_len,) = HEADER_STRUCT.unpack(header_data)
    if payload_len > MAX_MESSAGE_SIZE:
        raise ValueError(f"Message length {payload_len} exceeds maximum allowed size ({MAX_MESSAGE_SIZE} bytes)")

    body_data = _recv_exact(sock, payload_len)
    if body_data is None:
        return None

    try:
        data = json.loads(body_data.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Parsed JSON payload is not an object/dictionary")
        return data
    except Exception as e:
        raise ValueError(f"Malformed JSON received: {e}") from e


async def async_send_msg(writer: asyncio.StreamWriter, data: Dict[str, Any]) -> None:
    """
    Serialize data dictionary to JSON, prefix with a 4-byte length header,
    and transmit over an asyncio StreamWriter.
    """
    raw_payload = json.dumps(data).encode("utf-8")
    payload_len = len(raw_payload)
    if payload_len > MAX_MESSAGE_SIZE:
        raise ValueError(f"Payload size ({payload_len} bytes) exceeds maximum limit ({MAX_MESSAGE_SIZE} bytes)")

    header = HEADER_STRUCT.pack(payload_len)
    writer.write(header + raw_payload)
    await writer.drain()


async def async_recv_msg(reader: asyncio.StreamReader) -> Optional[Dict[str, Any]]:
    """
    Receive a length-prefixed JSON message from an asyncio StreamReader.
    Returns the parsed dictionary, or None if EOF is reached.
    """
    try:
        header_data = await reader.readexactly(HEADER_SIZE)
    except (asyncio.IncompleteReadError, ConnectionResetError, ConnectionAbortedError, OSError):
        return None

    (payload_len,) = HEADER_STRUCT.unpack(header_data)
    if payload_len > MAX_MESSAGE_SIZE:
        raise ValueError(f"Message length {payload_len} exceeds maximum allowed size ({MAX_MESSAGE_SIZE} bytes)")

    try:
        body_data = await reader.readexactly(payload_len)
    except (asyncio.IncompleteReadError, ConnectionResetError, ConnectionAbortedError, OSError):
        return None

    try:
        data = json.loads(body_data.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Parsed JSON payload is not an object/dictionary")
        return data
    except Exception as e:
        raise ValueError(f"Malformed JSON received: {e}") from e

