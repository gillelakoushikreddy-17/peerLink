"""
client.py - Terminal-based WebRTC Peer-to-Peer Chat Client.

Connects to a signaling server solely for peer discovery and WebRTC signaling exchange.
All chat communication is strictly direct peer-to-peer over WebRTC DataChannels using aiortc.
Zero server-side message relay.
"""

import os
import sys
import time
import json
import socket
import logging
import threading
import asyncio
from typing import Dict, Any, Optional, List

from aiortc import (
    RTCPeerConnection,
    RTCSessionDescription,
    RTCConfiguration,
    RTCIceServer,
    RTCDataChannel,
)
from aiortc.sdp import candidate_from_sdp, candidate_to_sdp

from config import AppConfig, get_lan_ip, validate_ip, validate_port, validate_host, validate_stun_urls
from protocol import (
    send_msg,
    recv_msg,
    async_send_msg,
    async_recv_msg,
    MAX_MESSAGE_SIZE,
    ACTION_REGISTER,
    ACTION_REGISTER_ACK,
    ACTION_REGISTER_ERROR,
    ACTION_LIST_USERS,
    ACTION_USER_LIST,
    ACTION_CONNECT_REQ,
    ACTION_PEER_INFO,
    ACTION_CONNECT_ERROR,
    ACTION_USER_DISCONNECTED,
    ACTION_WEBRTC_OFFER,
    ACTION_WEBRTC_ANSWER,
    ACTION_WEBRTC_ICE,
    ACTION_CHAT_MESSAGE,
    ACTION_PEER_DISCONNECT,
)

logger = logging.getLogger("p2p_chat.client")


class PeerClient:
    """
    WebRTC P2P Chat Client that:
    1. Connects to the signaling server for peer discovery and SDP/ICE signaling exchange.
    2. Establishes direct WebRTC DataChannels (using STUN for NAT traversal).
    3. Sends and receives chat messages strictly peer-to-peer through the DataChannel.
    4. Never relays chat messages through the signaling server.
    """

    def __init__(
        self,
        username: str,
        signaling_ip: str,
        signaling_port: int,
        listen_host: str = "0.0.0.0",
        listen_port: int = 6001,
        advertised_ip: Optional[str] = None,
        stun_server_url: Optional[str] = None,
        stun_server_urls: Optional[List[str]] = None,
    ):
        self.username = username
        self.signaling_ip = signaling_ip
        self.signaling_port = signaling_port
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.advertised_ip = advertised_ip or get_lan_ip()

        # Parse and validate STUN server URL(s)
        if stun_server_urls:
            self.stun_server_urls = stun_server_urls
        elif stun_server_url:
            self.stun_server_urls = validate_stun_urls(stun_server_url)
        else:
            self.stun_server_urls = ["stun:stun.l.google.com:19302"]
        self.stun_server_url = self.stun_server_urls[0]

        # WebRTC Configuration with validated STUN servers
        self.rtc_config = RTCConfiguration(
            iceServers=[RTCIceServer(urls=self.stun_server_urls)]
        )

        # Signaling connection state
        self.signaling_connected = False
        self.signaling_reader: Optional[asyncio.StreamReader] = None
        self.signaling_writer: Optional[asyncio.StreamWriter] = None
        self.signaling_task: Optional[asyncio.Task] = None

        # Dedicated background asyncio event loop for networking
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self.loop_thread.start()

        # WebRTC Peer Connections and DataChannels
        # target_user -> RTCPeerConnection
        self.peer_connections: Dict[str, RTCPeerConnection] = {}
        # target_user -> RTCDataChannel
        self.data_channels: Dict[str, RTCDataChannel] = {}
        # target_user -> asyncio.Event (signals when DataChannel is open or errored)
        self.channel_ready_events: Dict[str, asyncio.Event] = {}
        # target_user -> list of early RTCIceCandidate objects buffered before remote description is set
        self.pending_ice_candidates: Dict[str, List[Any]] = {}
        # target_user -> error message from signaling/ICE
        self.connection_errors: Dict[str, str] = {}

        # Set of tracked background asyncio tasks to prevent premature garbage collection
        self.background_tasks: set[asyncio.Task] = set()

        # Active direct P2P connections: username -> {"channel": RTCDataChannel, "pc": RTCPeerConnection, "connected_at": float}
        self.active_peers: Dict[str, Dict[str, Any]] = {}
        self.peers_lock = threading.Lock()

        # Active chat partner
        self.active_chat_partner: Optional[str] = None

        # Synchronization for signaling requests
        self.pending_requests: Dict[str, asyncio.Event] = {}
        self.response_data: Dict[str, Any] = {}

        # Console print lock
        self.print_lock = threading.Lock()
        self.is_running = True

    def _create_tracked_task(self, coro) -> asyncio.Task:
        """Create an asyncio task on the background loop with a strong reference to prevent GC."""
        task = self.loop.create_task(coro)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        return task

    def _run_async_loop(self) -> None:
        """Run the dedicated asyncio event loop in a background thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _run_coro(self, coro, timeout: Optional[float] = 12.0) -> Any:
        """Execute a coroutine on the background asyncio loop and wait for result."""
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return fut.result(timeout=timeout)

    def safe_print(self, message: str, end: str = "\n") -> None:
        """Thread-safe terminal printing that cleanly redraws the CLI prompt."""
        with self.print_lock:
            sys.stdout.write(f"\r\033[K{message}{end}")
            if self.is_running:
                prompt_partner = f" [{self.active_chat_partner}]" if self.active_chat_partner else ""
                sys.stdout.write(f"CLI{prompt_partner} > ")
            sys.stdout.flush()

    def start_peer_listener(self) -> None:
        """
        Retained for backward compatibility. In WebRTC, peer connectivity
        is initiated via signaling rather than an open inbound TCP listen port.
        """
        self.is_running = True
        logger.info("WebRTC P2P listener active (using STUN: %s)", self.stun_server_url)

    # -------------------------------------------------------------------------
    # Signaling Server Connection & Discovery
    # -------------------------------------------------------------------------
    def connect_to_signaling_server(self) -> bool:
        """Establish connection to signaling server and register this client."""
        try:
            return self._run_coro(self._async_connect_to_signaling_server(), timeout=8.0)
        except Exception as e:
            self.safe_print(f"[-] Could not connect to signaling server at {self.signaling_ip}:{self.signaling_port}: {e}")
            self.signaling_connected = False
            return False

    async def _async_connect_to_signaling_server(self) -> bool:
        """Async implementation of signaling server connection and registration."""
        try:
            self.signaling_reader, self.signaling_writer = await asyncio.wait_for(
                asyncio.open_connection(self.signaling_ip, self.signaling_port),
                timeout=5.0,
            )
            self.signaling_connected = True
        except Exception as e:
            logger.debug("Failed to connect to signaling server: %s", e)
            return False

        # Register username on signaling server
        reg_payload = {
            "type": ACTION_REGISTER,
            "username": self.username,
            "listen_port": self.listen_port,
            "advertised_ip": self.advertised_ip,
        }
        try:
            await async_send_msg(self.signaling_writer, reg_payload)
            resp = await asyncio.wait_for(async_recv_msg(self.signaling_reader), timeout=5.0)

            if not resp or resp.get("type") != ACTION_REGISTER_ACK:
                err = resp.get("error") if resp else "No response from signaling server."
                self.safe_print(f"[-] Registration failed: {err}")
                self.signaling_connected = False
                return False

            self.safe_print(
                f"[+] Registered as '{self.username}' on signaling server ({self.signaling_ip}:{self.signaling_port})"
            )
            # Start background signaling reader task
            self.signaling_task = asyncio.create_task(self._signaling_listen_loop())
            return True

        except Exception as e:
            self.safe_print(f"[-] Error during signaling registration: {e}")
            self.signaling_connected = False
            return False

    async def _send_signaling(self, msg: Dict[str, Any]) -> bool:
        """Send a message to the signaling server."""
        if not self.signaling_connected or not self.signaling_writer:
            return False
        try:
            await async_send_msg(self.signaling_writer, msg)
            return True
        except Exception as e:
            logger.debug("Failed to send signaling message: %s", e)
            return False

    async def _signaling_listen_loop(self) -> None:
        """Continuous background loop processing incoming signaling messages."""
        while self.is_running and self.signaling_connected:
            try:
                msg = await async_recv_msg(self.signaling_reader)
                if msg is None:
                    break

                action = msg.get("type")

                if action == ACTION_USER_LIST:
                    self.response_data[ACTION_USER_LIST] = msg.get("users", [])
                    if ACTION_USER_LIST in self.pending_requests:
                        self.pending_requests[ACTION_USER_LIST].set()

                elif action == ACTION_CONNECT_ERROR:
                    self.response_data[ACTION_CONNECT_ERROR] = msg
                    if ACTION_CONNECT_REQ in self.pending_requests:
                        self.pending_requests[ACTION_CONNECT_REQ].set()
                    # Also print error if related to a direct signaling attempt
                    err = msg.get("error", "Unknown signaling error")
                    target = msg.get("target_user")
                    if target:
                        self.connection_errors[target] = err
                        if target in self.channel_ready_events:
                            self.channel_ready_events[target].set()
                        self.safe_print(f"[-] Signaling notice regarding '{target}': {err}")

                elif action == ACTION_PEER_INFO:
                    direction = msg.get("direction")
                    if direction == "incoming_notice":
                        peer_name = msg.get("peer_user")
                        self.safe_print(f"[*] Peer '{peer_name}' is requesting a direct chat...")
                    else:
                        self.response_data[ACTION_PEER_INFO] = msg
                        if ACTION_PEER_INFO in self.pending_requests:
                            self.pending_requests[ACTION_PEER_INFO].set()

                elif action == ACTION_USER_DISCONNECTED:
                    left_user = msg.get("username")
                    self.safe_print(f"[*] Signaling Notice: User '{left_user}' has gone offline.")

                elif action == ACTION_WEBRTC_OFFER:
                    # Incoming WebRTC offer from a peer
                    self._create_tracked_task(self._handle_incoming_webrtc_offer(msg))

                elif action == ACTION_WEBRTC_ANSWER:
                    # Incoming WebRTC answer from a peer
                    self._create_tracked_task(self._handle_incoming_webrtc_answer(msg))

                elif action == ACTION_WEBRTC_ICE:
                    # Incoming WebRTC ICE candidate from a peer
                    self._create_tracked_task(self._handle_incoming_webrtc_ice(msg))

            except (ConnectionResetError, ConnectionAbortedError, asyncio.IncompleteReadError):
                break
            except Exception as e:
                logger.debug("Signaling listen loop exception: %s", e)
                break

        self.signaling_connected = False
        if self.is_running:
            self.safe_print("\n[!] Connection to signaling server lost. Direct WebRTC P2P chats remain active.")

    # -------------------------------------------------------------------------
    # WebRTC Peer Connection Establishment & Handshake
    # -------------------------------------------------------------------------
    def connect_to_peer(self, target_username: str) -> bool:
        """Synchronous wrapper to initiate a direct WebRTC connection to a target peer."""
        if target_username == self.username:
            self.safe_print("[-] You cannot chat with yourself.")
            return False

        with self.peers_lock:
            if target_username in self.active_peers:
                self.active_chat_partner = target_username
                self.safe_print(f"[+] Already connected to '{target_username}'. Active chat set to '{target_username}'.")
                return True

        if not self.signaling_connected:
            self.safe_print("[-] Signaling server unavailable. Cannot initiate WebRTC handshake.")
            return False

        try:
            return self._run_coro(self._async_connect_to_peer(target_username), timeout=15.0)
        except Exception as e:
            self.safe_print(f"[-] Connection attempt to '{target_username}' encountered an error: {e}")
            return False

    async def _async_connect_to_peer(self, target_username: str) -> bool:
        """Establish direct WebRTC peer connection via SDP offer and candidate exchange."""
        self.safe_print(f"[*] Initiating direct WebRTC P2P connection to '{target_username}'...")

        # If a previous connection attempt is hanging or closed, clean it up first
        await self._async_cleanup_peer(target_username)
        self.connection_errors.pop(target_username, None)

        # 1. Create RTCPeerConnection with STUN server configuration
        pc = RTCPeerConnection(configuration=self.rtc_config)
        self.peer_connections[target_username] = pc

        ready_event = asyncio.Event()
        self.channel_ready_events[target_username] = ready_event

        # 2. Create the DataChannel for chat messaging
        channel = pc.createDataChannel("chat")
        self.data_channels[target_username] = channel
        self._setup_datachannel(channel, target_username)
        self._setup_pc_events(pc, target_username)

        # 3. Create offer and set local description
        try:
            offer = await pc.createOffer()
            await pc.setLocalDescription(offer)
        except Exception as e:
            self.safe_print(f"[-] Failed to generate WebRTC offer: {e}")
            await self._async_cleanup_peer(target_username)
            return False

        # 4. Transmit offer via signaling server to peer
        sent = await self._send_signaling(
            {
                "type": ACTION_WEBRTC_OFFER,
                "target_user": target_username,
                "sdp": pc.localDescription.sdp,
                "type_name": pc.localDescription.type,
            }
        )
        if not sent:
            self.safe_print("[-] Failed to send WebRTC offer via signaling server.")
            await self._async_cleanup_peer(target_username)
            return False

        # 5. Extract and trickle gathered ICE candidates from local SDP
        await self._send_local_ice_candidates(pc, target_username)

        # 6. Wait for DataChannel to reach 'open' state (10s timeout)
        self.safe_print(f"[*] Waiting for direct P2P connectivity checks with '{target_username}'...")
        try:
            await asyncio.wait_for(ready_event.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            self.safe_print(
                f"\n[-] Direct WebRTC connection to '{target_username}' timed out.\n"
                f"    Reason: Could not establish a direct peer-to-peer path within 10 seconds.\n"
                f"    Possible Causes:\n"
                f"    1. Restrictive firewalls or symmetric NATs blocking direct UDP traversal.\n"
                f"    2. Peer '{target_username}' did not respond or is unreachable.\n"
                f"    Note: By design, chat messages are NEVER relayed through the signaling server.\n"
                f"    Troubleshooting:\n"
                f"    - Verify both devices allow outbound UDP on standard ports.\n"
                f"    - If on restrictive corporate/guest Wi-Fi with client isolation, test using standard networks or mobile hotspots."
            )
            await self._async_cleanup_peer(target_username)
            return False

        # Check if signaling server or ICE reported an immediate failure
        if target_username in self.connection_errors:
            err = self.connection_errors.pop(target_username)
            self.safe_print(f"[-] Could not connect to '{target_username}': {err}")
            await self._async_cleanup_peer(target_username)
            return False

        if channel.readyState != "open":
            self.safe_print(f"[-] DataChannel failed to open (state: {channel.readyState}).")
            await self._async_cleanup_peer(target_username)
            return False

        with self.peers_lock:
            self.active_chat_partner = target_username

        return True

    async def _handle_incoming_webrtc_offer(self, msg: Dict[str, Any]) -> None:
        """Handle incoming WebRTC offer from a remote peer."""
        sender = msg.get("sender")
        sdp = msg.get("sdp")
        type_name = msg.get("type_name", "offer")

        if not sender or not sdp:
            return

        # Check for glare (simultaneous outbound connection attempt)
        existing_pc = self.peer_connections.get(sender)
        if existing_pc and existing_pc.signalingState == "have-local-offer":
            # Polite / impolite tie-breaking: lower username yields
            if self.username < sender:
                # We yield to remote offer
                logger.debug("Glare resolution: yielding to remote offer from '%s'", sender)
                await self._async_cleanup_peer(sender)
            else:
                # Impolite: ignore remote offer and let remote peer accept ours
                logger.debug("Glare resolution: ignoring remote offer from '%s'", sender)
                return
        elif sender in self.active_peers:
            logger.debug("Ignoring duplicate offer from already connected peer '%s'", sender)
            return

        self.safe_print(f"\n[*] Received direct WebRTC chat request from '{sender}'. Connecting...")

        pc = RTCPeerConnection(configuration=self.rtc_config)
        self.peer_connections[sender] = pc

        ready_event = asyncio.Event()
        self.channel_ready_events[sender] = ready_event
        self._setup_pc_events(pc, sender)

        @pc.on("datachannel")
        def on_datachannel(channel: RTCDataChannel):
            logger.debug("Received inbound DataChannel from '%s'", sender)
            self.data_channels[sender] = channel
            self._setup_datachannel(channel, sender)

        try:
            remote_desc = RTCSessionDescription(sdp=sdp, type=type_name)
            await pc.setRemoteDescription(remote_desc)

            # Drain any early ICE candidates that arrived before remote description
            if sender in self.pending_ice_candidates:
                for cand in self.pending_ice_candidates[sender]:
                    try:
                        await pc.addIceCandidate(cand)
                    except Exception as e:
                        logger.debug("Error applying early candidate: %s", e)
                del self.pending_ice_candidates[sender]

            # Generate answer and set local description
            answer = await pc.createAnswer()
            await pc.setLocalDescription(answer)

            # Send answer to signaling server
            await self._send_signaling(
                {
                    "type": ACTION_WEBRTC_ANSWER,
                    "target_user": sender,
                    "sdp": pc.localDescription.sdp,
                    "type_name": pc.localDescription.type,
                }
            )

            # Send local ICE candidates to peer
            await self._send_local_ice_candidates(pc, sender)

        except Exception as e:
            self.safe_print(f"[-] Failed to process WebRTC offer from '{sender}': {e}")
            await self._async_cleanup_peer(sender)

    async def _handle_incoming_webrtc_answer(self, msg: Dict[str, Any]) -> None:
        """Handle incoming WebRTC answer from remote peer."""
        sender = msg.get("sender")
        sdp = msg.get("sdp")
        type_name = msg.get("type_name", "answer")

        if not sender or not sdp:
            return

        pc = self.peer_connections.get(sender)
        if not pc:
            logger.debug("Received answer for unknown or closed peer connection: %s", sender)
            return

        try:
            remote_desc = RTCSessionDescription(sdp=sdp, type=type_name)
            await pc.setRemoteDescription(remote_desc)

            # Drain buffered ICE candidates
            if sender in self.pending_ice_candidates:
                for cand in self.pending_ice_candidates[sender]:
                    try:
                        await pc.addIceCandidate(cand)
                    except Exception as e:
                        logger.debug("Error applying early candidate after answer: %s", e)
                del self.pending_ice_candidates[sender]

        except Exception as e:
            self.safe_print(f"[-] Failed to set remote answer from '{sender}': {e}")
            await self._async_cleanup_peer(sender)

    async def _handle_incoming_webrtc_ice(self, msg: Dict[str, Any]) -> None:
        """Handle trickle ICE candidate from signaling server."""
        sender = msg.get("sender")
        cand_data = msg.get("candidate")
        if not sender or not cand_data:
            return

        try:
            if isinstance(cand_data, dict):
                cand_str = cand_data.get("candidate", "")
                sdp_mid = str(cand_data.get("sdpMid", "0"))
                sdp_mline_index = int(cand_data.get("sdpMLineIndex", 0))
            elif isinstance(cand_data, str):
                cand_str = cand_data
                sdp_mid = "0"
                sdp_mline_index = 0
            else:
                return

            if not cand_str:
                return

            candidate_obj = candidate_from_sdp(cand_str)
            candidate_obj.sdpMid = sdp_mid
            candidate_obj.sdpMLineIndex = sdp_mline_index

            pc = self.peer_connections.get(sender)
            if pc and pc.remoteDescription is not None:
                await pc.addIceCandidate(candidate_obj)
            else:
                # Buffer candidate until remote description is set
                self.pending_ice_candidates.setdefault(sender, []).append(candidate_obj)

        except Exception as e:
            logger.debug("Could not parse or add ICE candidate from '%s': %s", sender, e)

    async def _send_local_ice_candidates(self, pc: RTCPeerConnection, target_user: str) -> None:
        """Extract gathered ICE candidates from local SDP and trickle them via signaling."""
        if not pc.localDescription or not pc.localDescription.sdp:
            return

        for line in pc.localDescription.sdp.split("\r\n"):
            if line.startswith("a=candidate:"):
                cand_str = line[2:]  # Remove 'a='
                try:
                    c = candidate_from_sdp(cand_str)
                    c.sdpMid = "0"
                    c.sdpMLineIndex = 0
                    await self._send_signaling(
                        {
                            "type": ACTION_WEBRTC_ICE,
                            "target_user": target_user,
                            "candidate": {
                                "candidate": candidate_to_sdp(c),
                                "sdpMid": c.sdpMid,
                                "sdpMLineIndex": c.sdpMLineIndex,
                            },
                        }
                    )
                except Exception as e:
                    logger.debug("Error serializing candidate: %s", e)

    def _setup_pc_events(self, pc: RTCPeerConnection, peer_user: str) -> None:
        """Monitor WebRTC peer connection and ICE state changes."""

        @pc.on("connectionstatechange")
        async def on_connectionstatechange():
            state = pc.connectionState
            logger.info("WebRTC connection state with '%s' changed to: %s", peer_user, state)
            if state == "failed":
                self.connection_errors[peer_user] = "WebRTC peer connection failed."
                if peer_user in self.channel_ready_events:
                    self.channel_ready_events[peer_user].set()
                self.safe_print(
                    f"\n[-] Direct WebRTC connection to '{peer_user}' failed.\n"
                    f"    (Direct UDP connectivity could not be established without relay.)"
                )
                await self._async_cleanup_peer(peer_user)
            elif state == "closed":
                await self._async_cleanup_peer(peer_user)

        @pc.on("iceconnectionstatechange")
        async def on_iceconnectionstatechange():
            state = pc.iceConnectionState
            logger.debug("ICE state with '%s': %s", peer_user, state)
            if state == "failed":
                self.connection_errors[peer_user] = "ICE connectivity checks failed."
                if peer_user in self.channel_ready_events:
                    self.channel_ready_events[peer_user].set()
                self.safe_print(
                    f"\n[-] Direct WebRTC ICE check with '{peer_user}' failed (no direct UDP path)."
                )
                await self._async_cleanup_peer(peer_user)

    def _setup_datachannel(self, channel: RTCDataChannel, peer_user: str) -> None:
        """Register event callbacks for the WebRTC chat DataChannel."""

        def handle_open():
            logger.info("DataChannel with '%s' is OPEN", peer_user)
            with self.peers_lock:
                self.active_peers[peer_user] = {
                    "channel": channel,
                    "pc": self.peer_connections.get(peer_user),
                    "connected_at": time.time(),
                }
                if not self.active_chat_partner:
                    self.active_chat_partner = peer_user

            if peer_user in self.channel_ready_events:
                self.channel_ready_events[peer_user].set()

            self.safe_print(
                f"\n[+] Direct WebRTC DataChannel established with '{peer_user}'! (Encrypted P2P)"
            )

        @channel.on("open")
        def on_open():
            handle_open()

        if channel.readyState == "open":
            handle_open()

        @channel.on("message")
        def on_message(raw_msg):
            try:
                if isinstance(raw_msg, bytes):
                    raw_msg = raw_msg.decode("utf-8")
                msg = json.loads(raw_msg)
            except Exception as e:
                logger.debug("Failed to decode DataChannel message from %s: %s", peer_user, e)
                return

            action = msg.get("type")
            if action == ACTION_CHAT_MESSAGE:
                sender = msg.get("from", peer_user)
                text = msg.get("text", "")
                ts = time.strftime("%H:%M:%S", time.localtime(msg.get("timestamp", time.time())))
                self.safe_print(f"\n[{ts}] [{sender}]: {text}")

            elif action == ACTION_PEER_DISCONNECT:
                self.safe_print(f"\n[*] Peer '{peer_user}' closed the direct chat session.")
                asyncio.run_coroutine_threadsafe(self._async_cleanup_peer(peer_user), self.loop)

        @channel.on("close")
        def on_close():
            logger.info("DataChannel with '%s' closed", peer_user)
            asyncio.run_coroutine_threadsafe(self._async_cleanup_peer(peer_user), self.loop)

    async def _async_cleanup_peer(self, peer_user: str) -> None:
        """Clean up all WebRTC resources associated with a specific peer."""
        had_peer = False
        with self.peers_lock:
            if peer_user in self.active_peers:
                del self.active_peers[peer_user]
                had_peer = True
            if self.active_chat_partner == peer_user:
                self.active_chat_partner = next(iter(self.active_peers)) if self.active_peers else None

        channel = self.data_channels.pop(peer_user, None)
        if channel:
            try:
                channel.close()
            except Exception:
                pass

        pc = self.peer_connections.pop(peer_user, None)
        if pc:
            try:
                await pc.close()
            except Exception:
                pass

        self.pending_ice_candidates.pop(peer_user, None)
        self.channel_ready_events.pop(peer_user, None)

        if had_peer:
            self.safe_print(f"\n[!] Direct chat session with '{peer_user}' disconnected.")

    # -------------------------------------------------------------------------
    # Chat and CLI Actions
    # -------------------------------------------------------------------------
    def send_chat_message(self, target_user: str, text: str) -> bool:
        """Send a message to a peer over the direct WebRTC DataChannel."""
        if not text.strip():
            return False

        # Guard against oversized messages that exceed max payload limit
        encoded_len = len(text.encode("utf-8"))
        if encoded_len > MAX_MESSAGE_SIZE - 512:
            self.safe_print(
                f"[-] Message rejected: payload size ({encoded_len} bytes) exceeds maximum limit ({MAX_MESSAGE_SIZE - 512} bytes)."
            )
            return False

        with self.peers_lock:
            peer_info = self.active_peers.get(target_user)

        if not peer_info:
            self.safe_print(f"[*] Not connected to '{target_user}'. Attempting direct WebRTC connection...")
            if not self.connect_to_peer(target_user):
                return False
            with self.peers_lock:
                peer_info = self.active_peers.get(target_user)
            if not peer_info:
                return False

        channel: RTCDataChannel = peer_info["channel"]
        if channel.readyState != "open":
            self.safe_print(f"[-] DataChannel to '{target_user}' is not open (state: {channel.readyState}).")
            return False

        payload = {
            "type": ACTION_CHAT_MESSAGE,
            "from": self.username,
            "to": target_user,
            "text": text,
            "timestamp": time.time(),
        }

        try:
            raw_payload = json.dumps(payload)
            self.loop.call_soon_threadsafe(channel.send, raw_payload)
            ts = time.strftime("%H:%M:%S")
            self.safe_print(f"[{ts}] [You -> {target_user}]: {text}")
            return True
        except Exception as e:
            self.safe_print(f"[-] Failed to send message to '{target_user}': {e}")
            self.close_peer(target_user)
            return False

    def list_online_users(self) -> None:
        """Request and print list of registered online users from the signaling server."""
        if not self.signaling_connected:
            self.safe_print("[-] Not connected to signaling server. Cannot retrieve online users.")
            return

        try:
            self._run_coro(self._async_list_online_users(), timeout=5.0)
        except Exception as e:
            self.safe_print(f"[-] Failed to retrieve user list: {e}")

    async def _async_list_online_users(self) -> None:
        """Async implementation to request user list from signaling server."""
        req_event = asyncio.Event()
        self.pending_requests[ACTION_USER_LIST] = req_event
        self.response_data.pop(ACTION_USER_LIST, None)

        sent = await self._send_signaling({"type": ACTION_LIST_USERS})
        if not sent:
            self.safe_print("[-] Failed to query signaling server.")
            return

        try:
            await asyncio.wait_for(req_event.wait(), timeout=4.0)
        except asyncio.TimeoutError:
            self.safe_print("[-] Timed out waiting for user list from signaling server.")
            return

        users = self.response_data.get(ACTION_USER_LIST, [])
        self.safe_print("\n" + "=" * 45)
        self.safe_print("              ONLINE USERS")
        self.safe_print("=" * 45)
        if not users:
            self.safe_print("  No other users currently registered online.")
        else:
            for u in users:
                uname = u.get("username")
                with self.peers_lock:
                    connected_status = " [DIRECT WEBRTC CONNECTED]" if uname in self.active_peers else ""
                self.safe_print(f"  • {uname:<15}{connected_status}")
        self.safe_print("=" * 45 + "\n")

    def show_status(self) -> None:
        """Display detailed connection and WebRTC state."""
        sig_status = "CONNECTED" if self.signaling_connected else "DISCONNECTED"
        self.safe_print("\n" + "=" * 50)
        self.safe_print("            CONNECTION STATUS")
        self.safe_print("=" * 50)
        self.safe_print(f"  Username:            {self.username}")
        self.safe_print(f"  Signaling Server:    {self.signaling_ip}:{self.signaling_port} ({sig_status})")
        self.safe_print(f"  STUN Server:         {self.stun_server_url}")
        self.safe_print(f"  Active Chat Partner: {self.active_chat_partner or 'None'}")

        with self.peers_lock:
            peers = list(self.active_peers.items())

        self.safe_print(f"  Direct WebRTC Peers: {len(peers)}")
        for uname, info in peers:
            channel: RTCDataChannel = info["channel"]
            pc: RTCPeerConnection = info["pc"]
            pc_state = pc.connectionState if pc else "unknown"
            ice_state = pc.iceConnectionState if pc else "unknown"
            dc_state = channel.readyState if channel else "unknown"
            self.safe_print(f"    - {uname:<12} (PC: {pc_state}, ICE: {ice_state}, Channel: {dc_state})")
        self.safe_print("=" * 50 + "\n")

    def show_help(self) -> None:
        """Display available CLI commands."""
        help_text = """
Commands:
  /users                 - List online users registered on signaling server
  /chat <username>       - Establish direct WebRTC P2P DataChannel to peer
  /msg <username> <text> - Send a direct message to a specific peer
  /status                - Display WebRTC connection status and active peers
  /close [username]      - Close direct WebRTC chat session with a peer
  /help                  - Show this help message
  /exit                  - Disconnect all sessions and exit

Tip: Once connected to a peer via /chat <username>, any text entered without
a leading '/' command is sent directly to your active chat partner over WebRTC.
"""
        self.safe_print(help_text)

    def close_peer(self, target_user: Optional[str]) -> None:
        """Close direct WebRTC connection with a peer."""
        target = target_user or self.active_chat_partner
        if not target:
            self.safe_print("[-] No peer specified and no active chat partner.")
            return

        with self.peers_lock:
            peer_info = self.active_peers.get(target)

        if not peer_info:
            self.safe_print(f"[-] Not directly connected to '{target}'.")
            return

        channel = peer_info.get("channel")
        if channel and channel.readyState == "open":
            try:
                disconnect_payload = json.dumps({"type": ACTION_PEER_DISCONNECT})
                self.loop.call_soon_threadsafe(channel.send, disconnect_payload)
            except Exception:
                pass

        try:
            self._run_coro(self._async_cleanup_peer(target), timeout=3.0)
        except Exception:
            pass

        self.safe_print(f"[+] Closed direct WebRTC connection with '{target}'.")

    def disconnect_all(self) -> None:
        """Cleanly close all WebRTC peer connections, signaling socket, and loop."""
        self.is_running = False

        try:
            self._run_coro(self._async_disconnect_all(), timeout=4.0)
        except Exception:
            pass

        # Stop asyncio event loop
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.loop_thread.join(timeout=2.0)
        if not self.loop.is_closed():
            try:
                self.loop.close()
            except Exception:
                pass

    async def _async_disconnect_all(self) -> None:
        """Async cleanup of all peer connections and signaling sockets."""
        peer_names = list(self.peer_connections.keys())
        for peer in peer_names:
            await self._async_cleanup_peer(peer)

        if self.signaling_task and not self.signaling_task.done():
            self.signaling_task.cancel()

        # Cancel and wait on any remaining background tasks
        for task in list(self.background_tasks):
            if not task.done():
                task.cancel()
        if self.background_tasks:
            await asyncio.gather(*list(self.background_tasks), return_exceptions=True)

        if self.signaling_writer:
            try:
                self.signaling_writer.close()
                await self.signaling_writer.wait_closed()
            except Exception:
                pass
            self.signaling_connected = False


# -----------------------------------------------------------------------------
# Interactive CLI Loop
# -----------------------------------------------------------------------------
def run_cli():
    """Interactive Command Line Interface entry point."""
    config = AppConfig()
    config.configure_logging("p2p_chat.client")

    print("=" * 55)
    print("       PYTHON P2P CLI CHAT (WebRTC DataChannel)")
    print("=" * 55)

    # 1. Username resolution: CLI argument > environment variable > interactive prompt
    username = ""
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        username = sys.argv[1].strip()
    elif config.client_username:
        username = config.client_username

    while not username:
        try:
            val = input("Enter username: ").strip()
            if len(val) >= 2 and val.replace("_", "").replace("-", "").isalnum():
                username = val
            else:
                print("Username must be at least 2 alphanumeric characters (dashes and underscores allowed).")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(0)

    # 2. Server host and port resolution
    server_ip = config.signaling_server_ip
    server_port = config.server_port

    if len(sys.argv) > 2:
        raw_target = sys.argv[2].strip()
        if ":" in raw_target:
            h, p = raw_target.rsplit(":", 1)
            try:
                server_ip = validate_host(h, "Signaling Server Host")
                server_port = validate_port(p, "Signaling Server Port")
            except ValueError as e:
                print(f"Warning: {e}. Using {server_ip}:{server_port}")
        else:
            try:
                server_ip = validate_host(raw_target, "Signaling Server Host")
            except ValueError as e:
                print(f"Warning: {e}. Falling back to configured host: {server_ip}")
    elif "SIGNALING_SERVER_IP" not in os.environ:
        try:
            prompt_str = f"Enter signaling server host/IP [{server_ip}]: "
            val = input(prompt_str).strip()
            if val:
                if ":" in val:
                    h, p = val.rsplit(":", 1)
                    try:
                        server_ip = validate_host(h, "Signaling Server Host")
                        server_port = validate_port(p, "Signaling Server Port")
                    except ValueError as e:
                        print(f"Warning: {e}. Using {server_ip}:{server_port}")
                else:
                    try:
                        server_ip = validate_host(val, "Signaling Server Host")
                    except ValueError as e:
                        print(f"Warning: {e}. Using {server_ip}")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(0)

    # 3. Optional signaling server port argument (e.g. python client.py alice 127.0.0.1 5050)
    listen_port = config.client_listen_port
    if len(sys.argv) > 3:
        try:
            server_port = validate_port(sys.argv[3], "Signaling Server Port")
        except ValueError as e:
            print(f"Warning: {e}. Using {server_port}")

    client = PeerClient(
        username=username,
        signaling_ip=server_ip,
        signaling_port=server_port,
        listen_host=config.client_listen_host,
        listen_port=listen_port,
        stun_server_url=config.stun_server_url,
    )
    client.start_peer_listener()

    # Connect to signaling server
    if not client.connect_to_signaling_server():
        print("[-] Could not connect to signaling server.")
        print("[*] Note: Discovery is disabled until signaling server is reached.")
    else:
        print("[+] Discovery service connected. Discovering online peers...")
        client.list_online_users()

    client.show_help()

    # Main CLI loop
    try:
        while client.is_running:
            partner_tag = f" [{client.active_chat_partner}]" if client.active_chat_partner else ""
            try:
                line = input(f"CLI{partner_tag} > ").strip()
            except EOFError:
                break

            if not line:
                continue

            if line.startswith("/"):
                parts = line.split(maxsplit=2)
                cmd = parts[0].lower()

                if cmd == "/users":
                    client.list_online_users()

                elif cmd == "/chat":
                    if len(parts) < 2:
                        print("Usage: /chat <username>")
                    else:
                        target = parts[1].strip()
                        client.connect_to_peer(target)

                elif cmd == "/msg":
                    if len(parts) < 3:
                        print("Usage: /msg <username> <message text>")
                    else:
                        target = parts[1].strip()
                        text = parts[2].strip()
                        client.send_chat_message(target, text)

                elif cmd == "/status":
                    client.show_status()

                elif cmd == "/close":
                    target = parts[1].strip() if len(parts) > 1 else None
                    client.close_peer(target)

                elif cmd == "/help":
                    client.show_help()

                elif cmd == "/exit":
                    print("[*] Exiting chat application...")
                    break

                else:
                    print(f"[-] Unknown command '{cmd}'. Type /help for available commands.")

            else:
                # Direct message to active partner
                if client.active_chat_partner:
                    client.send_chat_message(client.active_chat_partner, line)
                else:
                    print("[-] No active chat partner selected. Use /chat <username> or /msg <username> <text>")

    except KeyboardInterrupt:
        print("\n[*] Exiting chat application...")
    finally:
        if client:
            client.disconnect_all()
        print("[*] Goodbye!")


if __name__ == "__main__":
    run_cli()
