"""
server.py - P2P Signaling and Discovery Server.

Facilitates discovery and connection-metadata exchange between peers.
STRICT RULE: This server never receives, relays, broadcasts, or stores chat messages.
All chat communication is strictly direct peer-to-peer over separate TCP sockets.
"""

import sys
import time
import socket
import logging
import threading
import re
from typing import Dict, Any, Optional

from config import AppConfig, get_lan_ip
from protocol import (
    send_msg,
    recv_msg,
    ACTION_REGISTER,
    ACTION_REGISTER_ACK,
    ACTION_REGISTER_ERROR,
    ACTION_LIST_USERS,
    ACTION_USER_LIST,
    ACTION_CONNECT_REQ,
    ACTION_PEER_INFO,
    ACTION_CONNECT_ERROR,
    ACTION_USER_DISCONNECTED,
    ACTION_HEARTBEAT,
    ACTION_HEARTBEAT_ACK,
    ACTION_WEBRTC_OFFER,
    ACTION_WEBRTC_ANSWER,
    ACTION_WEBRTC_ICE,
    ACTION_CHAT_MESSAGE,
    ALLOWED_SIGNALING_ACTIONS,
)

logger = logging.getLogger("p2p_chat.server")

# Username validation pattern: 2 to 24 characters, letters, numbers, underscores, dashes
USERNAME_REGEX = re.compile(r"^[A-Za-z0-9_-]{2,24}$")


class SignalingServer:
    """
    Lightweight, multi-threaded TCP signaling server for peer discovery.
    Coordinates peer registration and connection requests only.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 5050):
        self.host = host
        self.port = port
        self.server_sock: Optional[socket.socket] = None
        self.is_running = False

        # Registry of connected clients
        # username -> {"ip": str, "listen_port": int, "socket": socket.socket, "connected_at": float}
        self.clients: Dict[str, Dict[str, Any]] = {}
        # socket -> username
        self.sock_to_username: Dict[socket.socket, str] = {}
        self.lock = threading.RLock()

    def start(self) -> None:
        """Bind server socket and begin listening for client connections."""
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sys.platform != "win32":
            self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        else:
            try:
                self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            except Exception:
                pass

        try:
            self.server_sock.bind((self.host, self.port))
            self.server_sock.listen(50)
            self.is_running = True
        except OSError as e:
            logger.error("Failed to bind signaling server on %s:%d: %s", self.host, self.port, e)
            if self.server_sock:
                self.server_sock.close()
            raise

        lan_ip = get_lan_ip()
        print("=" * 60)
        print("   PEERLINK — P2P SIGNALING SERVER (DISCOVERY ONLY)")
        print("=" * 60)
        print(f"[*] Bound on:         {self.host}:{self.port}")
        print(f"[*] Local LAN IP:     {lan_ip}")
        print(f"[*] Port:             {self.port}")
        print(f"[*] Other devices on the Wi-Fi should connect to: {lan_ip}")
        print("[*] Note: Chat messages are NEVER relayed through this server.")
        print("=" * 60)
        print("[*] Waiting for client connections... Press Ctrl+C to stop.\n")

        self._accept_loop()

    def _accept_loop(self) -> None:
        """Main loop accepting incoming client connections."""
        while self.is_running:
            try:
                client_sock, client_addr = self.server_sock.accept()
                logger.info("Accepted signaling connection from %s:%d", client_addr[0], client_addr[1])
                t = threading.Thread(
                    target=self._handle_client,
                    args=(client_sock, client_addr),
                    daemon=True,
                )
                t.start()
            except OSError:
                # Occurs when socket is closed during shutdown
                break

    def _safe_send_msg(self, sock: socket.socket, data: Dict[str, Any], lock: Optional[threading.Lock] = None) -> bool:
        """Thread-safe socket transmission to prevent interleaved TCP framing."""
        if not lock:
            with self.lock:
                username = self.sock_to_username.get(sock)
                if username and username in self.clients:
                    lock = self.clients[username].get("send_lock")
        if lock:
            with lock:
                try:
                    send_msg(sock, data)
                    return True
                except Exception:
                    return False
        else:
            try:
                send_msg(sock, data)
                return True
            except Exception:
                return False

    def _handle_client(self, client_sock: socket.socket, client_addr: tuple) -> None:
        """Handle signaling requests from a single client connection."""
        client_ip = client_addr[0]
        registered_user: Optional[str] = None

        try:
            while self.is_running:
                msg = recv_msg(client_sock)
                if msg is None:
                    # Client closed signaling connection
                    break

                action = msg.get("type")

                # STRICT SECURITY CHECK: Reject chat messages from traversing the signaling server
                if action == ACTION_CHAT_MESSAGE:
                    logger.warning(
                        "Client %s attempted forbidden chat relay action: %s",
                        registered_user or client_ip,
                        action,
                    )
                    self._safe_send_msg(
                        client_sock,
                        {
                            "type": ACTION_CONNECT_ERROR,
                            "error": "Forbidden: Signaling server handles discovery only. Chat messages must be direct P2P.",
                        },
                    )
                    continue

                # Reject unknown or unauthorized actions
                if not action or action not in ALLOWED_SIGNALING_ACTIONS:
                    logger.warning(
                        "Client %s attempted invalid or unknown action: %s",
                        registered_user or client_ip,
                        action,
                    )
                    self._safe_send_msg(
                        client_sock,
                        {
                            "type": ACTION_CONNECT_ERROR,
                            "error": f"Invalid action: '{action}' is not supported.",
                        },
                    )
                    continue

                if action == ACTION_REGISTER:
                    registered_user = self._handle_register(client_sock, client_ip, msg)
                elif action == ACTION_LIST_USERS:
                    self._handle_list_users(client_sock, registered_user)
                elif action == ACTION_CONNECT_REQ:
                    self._handle_connect_req(client_sock, registered_user, msg)
                elif action in (ACTION_WEBRTC_OFFER, ACTION_WEBRTC_ANSWER, ACTION_WEBRTC_ICE):
                    self._handle_webrtc_signaling(client_sock, registered_user, msg, action)
                elif action == ACTION_HEARTBEAT:
                    self._safe_send_msg(client_sock, {"type": ACTION_HEARTBEAT_ACK, "timestamp": time.time()})

        except (ConnectionResetError, ConnectionAbortedError) as e:
            logger.debug("Client connection reset: %s (%s)", registered_user or client_ip, e)
        except Exception as e:
            logger.error("Error handling client %s: %s", registered_user or client_ip, e, exc_info=True)
        finally:
            self._cleanup_client(client_sock, registered_user)

    def _handle_register(self, client_sock: socket.socket, client_ip: str, msg: Dict[str, Any]) -> Optional[str]:
        """Handle client registration with unique username and optional listening port."""
        username = str(msg.get("username", "")).strip()
        listen_port = msg.get("listen_port")

        if not USERNAME_REGEX.match(username):
            self._safe_send_msg(
                client_sock,
                {
                    "type": ACTION_REGISTER_ERROR,
                    "error": "Invalid username. Must be 2-24 alphanumeric characters, dashes, or underscores.",
                },
            )
            return None

        if listen_port is not None:
            try:
                listen_port = int(listen_port)
                if not (1024 <= listen_port <= 65535) and listen_port != 0:
                    raise ValueError()
            except (TypeError, ValueError):
                self._safe_send_msg(
                    client_sock,
                    {
                        "type": ACTION_REGISTER_ERROR,
                        "error": "Invalid listen_port. Must be an integer between 1024 and 65535.",
                    },
                )
                return None
        else:
            listen_port = 0

        # Allow client to suggest its own LAN IP if it knows it, but default to the socket's peer IP
        advertised_ip = msg.get("advertised_ip")
        effective_ip = client_ip
        if client_ip in ("127.0.0.1", "::1") and advertised_ip and advertised_ip != "0.0.0.0":
            effective_ip = advertised_ip

        is_error = False
        err_msg = ""
        with self.lock:
            # Reject re-registration on an already-registered socket connection
            existing_user = self.sock_to_username.get(client_sock)
            if existing_user:
                is_error = True
                err_msg = f"Connection already registered as '{existing_user}'."
            elif username in self.clients:
                is_error = True
                err_msg = f"Username '{username}' is already in use by another user."
            else:
                send_lock = threading.Lock()
                self.clients[username] = {
                    "ip": effective_ip,
                    "listen_port": listen_port,
                    "socket": client_sock,
                    "send_lock": send_lock,
                    "connected_at": time.time(),
                }
                self.sock_to_username[client_sock] = username

        if is_error:
            self._safe_send_msg(
                client_sock,
                {
                    "type": ACTION_REGISTER_ERROR,
                    "error": err_msg,
                },
            )
            return existing_user if existing_user else None

        logger.info("Registered user '%s' from %s (P2P listen port: %d)", username, effective_ip, listen_port)
        self._safe_send_msg(
            client_sock,
            {
                "type": ACTION_REGISTER_ACK,
                "username": username,
                "registered_ip": effective_ip,
                "listen_port": listen_port,
            },
            lock=send_lock,
        )
        return username

    def _handle_list_users(self, client_sock: socket.socket, requester: Optional[str]) -> None:
        """Send list of all other online users with their connection endpoints."""
        with self.lock:
            user_list = [
                {
                    "username": uname,
                    "ip": info["ip"],
                    "port": info["listen_port"],
                }
                for uname, info in self.clients.items()
                if uname != requester
            ]

        self._safe_send_msg(client_sock, {"type": ACTION_USER_LIST, "users": user_list})

    def _handle_connect_req(self, client_sock: socket.socket, requester: Optional[str], msg: Dict[str, Any]) -> None:
        """Handle request to obtain target peer's IP and listening port for direct connection."""
        if not requester:
            self._safe_send_msg(
                client_sock,
                {"type": ACTION_CONNECT_ERROR, "error": "Must be registered to request connection."},
            )
            return

        target_user = str(msg.get("target_user", "")).strip()
        if not USERNAME_REGEX.match(target_user):
            self._safe_send_msg(
                client_sock,
                {"type": ACTION_CONNECT_ERROR, "error": f"Invalid target_user format: '{target_user}'."},
            )
            return

        with self.lock:
            target_info = self.clients.get(target_user)
            requester_info = self.clients.get(requester)

        if not target_info:
            self._safe_send_msg(
                client_sock,
                {
                    "type": ACTION_CONNECT_ERROR,
                    "target_user": target_user,
                    "error": f"User '{target_user}' is not online or not registered.",
                },
            )
            return

        # Send peer connection metadata back to requester
        self._safe_send_msg(
            client_sock,
            {
                "type": ACTION_PEER_INFO,
                "peer_user": target_user,
                "peer_ip": target_info["ip"],
                "peer_port": target_info["listen_port"],
            },
        )

        # Notify target peer that requester is attempting to connect
        if requester_info:
            target_sock = target_info.get("socket")
            target_lock = target_info.get("send_lock")
            if target_sock:
                self._safe_send_msg(
                    target_sock,
                    {
                        "type": ACTION_PEER_INFO,
                        "peer_user": requester,
                        "peer_ip": requester_info["ip"],
                        "peer_port": requester_info["listen_port"],
                        "direction": "incoming_notice",
                    },
                    lock=target_lock,
                )

    def _handle_webrtc_signaling(
        self,
        client_sock: socket.socket,
        registered_user: Optional[str],
        msg: Dict[str, Any],
        action_type: str,
    ) -> None:
        """Route WebRTC signaling message (offer, answer, or candidate) to the target recipient."""
        if not registered_user:
            self._safe_send_msg(
                client_sock,
                {"type": ACTION_CONNECT_ERROR, "error": "Must be registered to send WebRTC signaling messages."},
            )
            return

        target_user = str(msg.get("target_user", "")).strip()
        if not target_user:
            self._safe_send_msg(
                client_sock,
                {"type": ACTION_CONNECT_ERROR, "error": "Missing target_user for WebRTC signaling."},
            )
            return

        if not USERNAME_REGEX.match(target_user):
            self._safe_send_msg(
                client_sock,
                {"type": ACTION_CONNECT_ERROR, "error": f"Invalid target_user format: '{target_user}'."},
            )
            return

        if target_user == registered_user:
            self._safe_send_msg(
                client_sock,
                {"type": ACTION_CONNECT_ERROR, "error": "Cannot signal oneself."},
            )
            return

        # Validate WebRTC payloads to prevent sending malformed signaling to peers
        if action_type in (ACTION_WEBRTC_OFFER, ACTION_WEBRTC_ANSWER):
            sdp = msg.get("sdp")
            type_name = msg.get("type_name")
            if not sdp or not isinstance(sdp, str) or not sdp.strip():
                self._safe_send_msg(
                    client_sock,
                    {"type": ACTION_CONNECT_ERROR, "target_user": target_user, "error": f"Missing or invalid SDP in {action_type}."},
                )
                return
            if not type_name or not isinstance(type_name, str):
                self._safe_send_msg(
                    client_sock,
                    {"type": ACTION_CONNECT_ERROR, "target_user": target_user, "error": f"Missing or invalid type_name in {action_type}."},
                )
                return
        elif action_type == ACTION_WEBRTC_ICE:
            candidate = msg.get("candidate")
            if not candidate or not isinstance(candidate, (dict, str)):
                self._safe_send_msg(
                    client_sock,
                    {"type": ACTION_CONNECT_ERROR, "target_user": target_user, "error": "Missing or invalid candidate in webrtc_ice."},
                )
                return

        with self.lock:
            target_info = self.clients.get(target_user)

        if not target_info:
            self._safe_send_msg(
                client_sock,
                {
                    "type": ACTION_CONNECT_ERROR,
                    "target_user": target_user,
                    "error": f"User '{target_user}' is not online or not registered.",
                },
            )
            return

        target_sock = target_info.get("socket")
        target_lock = target_info.get("send_lock")
        if not target_sock:
            return

        # Stamp the verified sender to prevent impersonation
        forwarded_payload = {
            "type": action_type,
            "sender": registered_user,
        }
        if "sdp" in msg:
            forwarded_payload["sdp"] = msg["sdp"]
        if "type_name" in msg:
            forwarded_payload["type_name"] = msg["type_name"]
        if "candidate" in msg:
            forwarded_payload["candidate"] = msg["candidate"]

        success = self._safe_send_msg(target_sock, forwarded_payload, lock=target_lock)
        if success:
            logger.debug("Routed %s from '%s' to '%s'", action_type, registered_user, target_user)
        else:
            logger.error("Failed to route %s from '%s' to '%s'", action_type, registered_user, target_user)
            self._safe_send_msg(
                client_sock,
                {
                    "type": ACTION_CONNECT_ERROR,
                    "target_user": target_user,
                    "error": f"Failed to deliver signaling message to '{target_user}'.",
                },
            )

    def _cleanup_client(self, client_sock: socket.socket, username: Optional[str]) -> None:
        """Remove client from registry, notify remaining peers, and close socket."""
        disconnected_user = username
        with self.lock:
            if not disconnected_user:
                disconnected_user = self.sock_to_username.get(client_sock)

            if disconnected_user and disconnected_user in self.clients:
                del self.clients[disconnected_user]
            if client_sock in self.sock_to_username:
                del self.sock_to_username[client_sock]

            remaining = [(info["socket"], info.get("send_lock")) for info in self.clients.values()]

        try:
            client_sock.close()
        except Exception:
            pass

        if disconnected_user:
            logger.info("User '%s' disconnected from signaling server", disconnected_user)
            # Notify remaining clients
            disconnect_msg = {
                "type": ACTION_USER_DISCONNECTED,
                "username": disconnected_user,
            }
            for s, slock in remaining:
                self._safe_send_msg(s, disconnect_msg, lock=slock)

    def stop(self) -> None:
        """Gracefully stop server and close all active sockets."""
        self.is_running = False
        with self.lock:
            for info in self.clients.values():
                try:
                    info["socket"].close()
                except Exception:
                    pass
            self.clients.clear()
            self.sock_to_username.clear()

        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass
        logger.info("Signaling server stopped")


def main():
    """Command-line entry point for signaling server."""
    config = AppConfig()
    config.configure_logging("p2p_chat.server")

    # Override with command-line arguments if provided: python server.py [host] [port]
    host = config.server_host
    port = config.server_port

    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            port = int(sys.argv[2])
        except ValueError:
            print(f"Error: Invalid port argument '{sys.argv[2]}'")
            sys.exit(1)

    server = SignalingServer(host=host, port=port)
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n[*] Shutting down signaling server...")
    finally:
        server.stop()
        print("[*] Server shutdown complete.")


if __name__ == "__main__":
    main()
