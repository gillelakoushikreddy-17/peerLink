"""
test_chat.py - Comprehensive Automated Test Suite for WebRTC P2P CLI Chat Application.

Verifies:
1. Configuration loading, environment overrides, IP/port validation, and host/domain validation.
2. TCP length-prefixed framing and message serialization (synchronous and asynchronous).
3. Signaling server operations:
   - Registration and duplicate username prevention
   - Online-user discovery (list_users)
   - Routing WebRTC offers, answers, and ICE candidates
   - Rejection of unauthenticated signaling requests
   - Rejection of self-signaling and non-existent targets
   - Critical Security: Strict server-side rejection of chat messages (zero relay)
   - Session disconnect notifications and cleanup
4. Direct WebRTC Peer-to-Peer DataChannel Communication:
   - Offer/answer exchange and ICE gathering via STUN
   - Direct DataChannel establishment
   - Bidirectional chat messaging over direct WebRTC DataChannel
   - Multi-peer concurrent chat sessions
   - Duplicate connection prevention
   - Connection timeout handling and resource cleanup
   - Disconnection and clean reconnection
5. Repository and security file hygiene (.gitignore, .env.example with STUN config).
"""

import os
import time
import socket
import asyncio
import unittest
import tempfile
import threading

from config import (
    AppConfig,
    load_dotenv,
    validate_ip,
    validate_port,
    validate_host,
    validate_stun_urls,
    parse_bool,
    get_lan_ip,
)
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
    ACTION_CHAT_MESSAGE,
    ACTION_WEBRTC_OFFER,
    ACTION_WEBRTC_ANSWER,
    ACTION_WEBRTC_ICE,
)
from server import SignalingServer
from client import PeerClient


def get_free_port() -> int:
    """Find an available port on loopback."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestConfigurationAndValidation(unittest.TestCase):
    """Test configuration parsing, .env loader, and input validation."""

    def test_validate_ip(self):
        self.assertEqual(validate_ip("192.168.1.1"), "192.168.1.1")
        self.assertEqual(validate_ip("10.0.0.5"), "10.0.0.5")
        self.assertEqual(validate_ip("127.0.0.1"), "127.0.0.1")
        self.assertEqual(validate_ip("0.0.0.0", allow_all_interfaces=True), "0.0.0.0")

        with self.assertRaises(ValueError):
            validate_ip("256.0.0.1")
        with self.assertRaises(ValueError):
            validate_ip("invalid_ip_string")
        with self.assertRaises(ValueError):
            validate_ip("192.168.1")
        with self.assertRaises(ValueError):
            validate_ip("0.0.0.0", allow_all_interfaces=False)

    def test_validate_host(self):
        # Valid IPs and domain names
        self.assertEqual(validate_host("127.0.0.1"), "127.0.0.1")
        self.assertEqual(validate_host("::1"), "::1")
        self.assertEqual(validate_host("localhost"), "localhost")
        self.assertEqual(validate_host("signal.example.com"), "signal.example.com")
        self.assertEqual(validate_host("chat-server-01.us-west-2.compute.internal"), "chat-server-01.us-west-2.compute.internal")

        # Invalid host inputs
        with self.assertRaises(ValueError):
            validate_host("")
        with self.assertRaises(ValueError):
            validate_host("bad host name with spaces")
        with self.assertRaises(ValueError):
            validate_host("server$name")

    def test_validate_port(self):
        self.assertEqual(validate_port(5000), 5000)
        self.assertEqual(validate_port("6001"), 6001)
        self.assertEqual(validate_port(1024), 1024)
        self.assertEqual(validate_port(65535), 65535)

        with self.assertRaises(ValueError):
            validate_port(80)
        with self.assertRaises(ValueError):
            validate_port(70000)
        with self.assertRaises(ValueError):
            validate_port("not_a_number")

    def test_parse_bool(self):
        self.assertTrue(parse_bool("true"))
        self.assertTrue(parse_bool("True"))
        self.assertTrue(parse_bool("1"))
        self.assertTrue(parse_bool("yes"))
        self.assertFalse(parse_bool("false"))
        self.assertFalse(parse_bool("0"))
        self.assertFalse(parse_bool("no"))
        self.assertFalse(parse_bool(None, default=False))
    def test_validate_stun_urls(self):
        # Default fallback
        self.assertEqual(validate_stun_urls(None), ["stun:stun.l.google.com:19302"])
        self.assertEqual(validate_stun_urls(""), ["stun:stun.l.google.com:19302"])

        # Single and multiple valid STUN/STUNS URLs
        self.assertEqual(
            validate_stun_urls("stun:stun1.l.google.com:19302, stun:stun2.l.google.com:19302"),
            ["stun:stun1.l.google.com:19302", "stun:stun2.l.google.com:19302"],
        )
        self.assertEqual(
            validate_stun_urls("stuns:stun.cloudflare.com:3478"),
            ["stuns:stun.cloudflare.com:3478"],
        )

        # Rejection of invalid protocols (no HTTP or TURN allowed)
        with self.assertRaises(ValueError):
            validate_stun_urls("http://not-stun.com")
        with self.assertRaises(ValueError):
            validate_stun_urls("turn:turn.example.com:3478")

    def test_load_dotenv(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            f.write("# Sample comment\n")
            f.write("TEST_P2P_VAR1=helloworld\n")
            f.write("TEST_P2P_VAR2=\"quoted_value\"\n")
            f.write("TEST_P2P_VAR3='single_quoted'\n")
            f.write("   # Indented comment\n")
            f.write("TEST_P2P_PORT=5555\n")
            temp_path = f.name

        try:
            os.environ.pop("TEST_P2P_VAR1", None)
            os.environ.pop("TEST_P2P_VAR2", None)
            os.environ.pop("TEST_P2P_VAR3", None)
            os.environ.pop("TEST_P2P_PORT", None)

            loaded = load_dotenv(temp_path)
            self.assertEqual(loaded["TEST_P2P_VAR1"], "helloworld")
            self.assertEqual(loaded["TEST_P2P_VAR2"], "quoted_value")
            self.assertEqual(loaded["TEST_P2P_VAR3"], "single_quoted")
            self.assertEqual(loaded["TEST_P2P_PORT"], "5555")
            self.assertEqual(os.environ.get("TEST_P2P_VAR1"), "helloworld")

            os.environ["TEST_P2P_OVERRIDE"] = "original_env_val"
            with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f2:
                f2.write("TEST_P2P_OVERRIDE=dotenv_val\n")
                temp_path2 = f2.name
            try:
                load_dotenv(temp_path2)
                self.assertEqual(os.environ.get("TEST_P2P_OVERRIDE"), "original_env_val")
            finally:
                if os.path.exists(temp_path2):
                    os.remove(temp_path2)
                os.environ.pop("TEST_P2P_OVERRIDE", None)

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            os.environ.pop("TEST_P2P_VAR1", None)
            os.environ.pop("TEST_P2P_VAR2", None)
            os.environ.pop("TEST_P2P_VAR3", None)
            os.environ.pop("TEST_P2P_PORT", None)

    def test_lan_ip_detection(self):
        ip = get_lan_ip()
        self.assertIsInstance(ip, str)
        self.assertGreater(len(ip), 6)
        validate_ip(ip, allow_all_interfaces=False)


class TestProtocolAndFraming(unittest.TestCase):
    """Test TCP length-prefixed message framing in sync and async modes."""

    def test_send_and_recv_msg_roundtrip(self):
        port = get_free_port()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", port))
        srv.listen(1)

        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cli.connect(("127.0.0.1", port))
        conn, _ = srv.accept()

        try:
            payload = {"action": "test", "number": 42, "text": "Hello, P2P World! 🚀"}
            send_msg(cli, payload)
            received = recv_msg(conn)
            self.assertEqual(received, payload)
        finally:
            conn.close()
            cli.close()
            srv.close()

    def test_async_send_and_recv_msg_roundtrip(self):
        async def _test():
            port = get_free_port()
            server_received = asyncio.Future()

            async def handle_client(reader, writer):
                msg = await async_recv_msg(reader)
                server_received.set_result(msg)
                await async_send_msg(writer, {"ack": True})
                writer.close()
                await writer.wait_closed()

            srv = await asyncio.start_server(handle_client, "127.0.0.1", port)
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                test_payload = {"type": "async_test", "data": [1, 2, 3]}
                await async_send_msg(writer, test_payload)

                received_at_server = await asyncio.wait_for(server_received, timeout=2.0)
                self.assertEqual(received_at_server, test_payload)

                ack = await asyncio.wait_for(async_recv_msg(reader), timeout=2.0)
                self.assertEqual(ack, {"ack": True})
                writer.close()
                await writer.wait_closed()
            finally:
                srv.close()
                await srv.wait_closed()

        asyncio.run(_test())

    def test_oversized_payload_rejection(self):
        dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            huge_dict = {"data": "A" * (MAX_MESSAGE_SIZE + 10)}
            with self.assertRaises(ValueError):
                send_msg(dummy_sock, huge_dict)
        finally:
            dummy_sock.close()


class TestSignalingServer(unittest.TestCase):
    """Test signaling server operations: registration, discovery, WebRTC routing, security."""

    @classmethod
    def setUpClass(cls):
        cls.server_port = get_free_port()
        cls.server = SignalingServer(host="127.0.0.1", port=cls.server_port)
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_registration_and_duplicate_rejection(self):
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s1, {"type": ACTION_REGISTER, "username": "alice", "listen_port": 6101})
            resp1 = recv_msg(s1)
            self.assertEqual(resp1.get("type"), ACTION_REGISTER_ACK)
            self.assertEqual(resp1.get("username"), "alice")

            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2.connect(("127.0.0.1", self.server_port))
            try:
                send_msg(s2, {"type": ACTION_REGISTER, "username": "alice", "listen_port": 6102})
                resp2 = recv_msg(s2)
                self.assertEqual(resp2.get("type"), ACTION_REGISTER_ERROR)
                self.assertIn("already in use", resp2.get("error", ""))

                send_msg(s2, {"type": ACTION_REGISTER, "username": "bob", "listen_port": 6102})
                resp2_ok = recv_msg(s2)
                self.assertEqual(resp2_ok.get("type"), ACTION_REGISTER_ACK)
                self.assertEqual(resp2_ok.get("username"), "bob")

            finally:
                s2.close()
                time.sleep(0.05)

        finally:
            s1.close()
            time.sleep(0.05)

    def test_online_user_discovery(self):
        s_alice = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_bob = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        s_alice.connect(("127.0.0.1", self.server_port))
        s_bob.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s_alice, {"type": ACTION_REGISTER, "username": "alice_disc", "listen_port": 6201})
            _ = recv_msg(s_alice)

            send_msg(s_bob, {"type": ACTION_REGISTER, "username": "bob_disc", "listen_port": 6202})
            _ = recv_msg(s_bob)

            send_msg(s_alice, {"type": ACTION_LIST_USERS})
            list_resp = recv_msg(s_alice)
            self.assertEqual(list_resp.get("type"), ACTION_USER_LIST)

            users = [u["username"] for u in list_resp.get("users", [])]
            self.assertIn("bob_disc", users)
            self.assertNotIn("alice_disc", users)

        finally:
            s_alice.close()
            s_bob.close()
            time.sleep(0.05)

    def test_webrtc_offer_and_answer_routing(self):
        """Test signaling server correctly routes WebRTC offers and answers between peers."""
        s_alice = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_bob = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        s_alice.connect(("127.0.0.1", self.server_port))
        s_bob.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s_alice, {"type": ACTION_REGISTER, "username": "alice_webrtc", "listen_port": 6301})
            _ = recv_msg(s_alice)
            send_msg(s_bob, {"type": ACTION_REGISTER, "username": "bob_webrtc", "listen_port": 6302})
            _ = recv_msg(s_bob)

            # Alice sends WebRTC offer intended for Bob
            send_msg(
                s_alice,
                {
                    "type": ACTION_WEBRTC_OFFER,
                    "target_user": "bob_webrtc",
                    "sdp": "v=0\r\no=alice 123 456\r\ns=TestOffer\r\n",
                    "type_name": "offer",
                },
            )

            # Bob receives routed offer with verified sender stamped as 'alice_webrtc'
            bob_received = recv_msg(s_bob)
            self.assertEqual(bob_received.get("type"), ACTION_WEBRTC_OFFER)
            self.assertEqual(bob_received.get("sender"), "alice_webrtc")
            self.assertEqual(bob_received.get("type_name"), "offer")
            self.assertIn("TestOffer", bob_received.get("sdp", ""))

            # Bob sends WebRTC answer back to Alice
            send_msg(
                s_bob,
                {
                    "type": ACTION_WEBRTC_ANSWER,
                    "target_user": "alice_webrtc",
                    "sdp": "v=0\r\no=bob 789 101\r\ns=TestAnswer\r\n",
                    "type_name": "answer",
                },
            )

            # Alice receives routed answer with verified sender stamped as 'bob_webrtc'
            alice_received = recv_msg(s_alice)
            self.assertEqual(alice_received.get("type"), ACTION_WEBRTC_ANSWER)
            self.assertEqual(alice_received.get("sender"), "bob_webrtc")
            self.assertEqual(alice_received.get("type_name"), "answer")
            self.assertIn("TestAnswer", alice_received.get("sdp", ""))

        finally:
            s_alice.close()
            s_bob.close()
            time.sleep(0.05)

    def test_webrtc_ice_candidate_routing(self):
        """Test signaling server routes trickle ICE candidates to target peer."""
        s_alice = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s_bob = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        s_alice.connect(("127.0.0.1", self.server_port))
        s_bob.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s_alice, {"type": ACTION_REGISTER, "username": "alice_ice", "listen_port": 6311})
            _ = recv_msg(s_alice)
            send_msg(s_bob, {"type": ACTION_REGISTER, "username": "bob_ice", "listen_port": 6312})
            _ = recv_msg(s_bob)

            # Alice sends ICE candidate to Bob
            sample_candidate = {
                "candidate": "candidate:1 1 UDP 2130706431 192.168.1.10 50000 typ host",
                "sdpMid": "0",
                "sdpMLineIndex": 0,
            }
            send_msg(
                s_alice,
                {
                    "type": ACTION_WEBRTC_ICE,
                    "target_user": "bob_ice",
                    "candidate": sample_candidate,
                },
            )

            bob_recv = recv_msg(s_bob)
            self.assertEqual(bob_recv.get("type"), ACTION_WEBRTC_ICE)
            self.assertEqual(bob_recv.get("sender"), "alice_ice")
            self.assertEqual(bob_recv.get("candidate"), sample_candidate)

        finally:
            s_alice.close()
            s_bob.close()
            time.sleep(0.05)

    def test_unregistered_client_signaling_rejected(self):
        """Test that an unregistered connection cannot route signaling messages."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s, {"type": ACTION_WEBRTC_OFFER, "target_user": "anyone", "sdp": "...", "type_name": "offer"})
            resp = recv_msg(s)
            self.assertEqual(resp.get("type"), ACTION_CONNECT_ERROR)
            self.assertIn("Must be registered", resp.get("error", ""))
        finally:
            s.close()
            time.sleep(0.05)

    def test_nonexistent_target_signaling_error(self):
        """Test sending signaling messages to an offline user returns an error."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s, {"type": ACTION_REGISTER, "username": "alice_alone", "listen_port": 6321})
            _ = recv_msg(s)

            send_msg(s, {"type": ACTION_WEBRTC_OFFER, "target_user": "ghost_user", "sdp": "...", "type_name": "offer"})
            resp = recv_msg(s)
            self.assertEqual(resp.get("type"), ACTION_CONNECT_ERROR)
            self.assertIn("not online", resp.get("error", ""))
        finally:
            s.close()
            time.sleep(0.05)

    def test_self_signaling_rejected(self):
        """Test that a client cannot send WebRTC signaling to itself."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s, {"type": ACTION_REGISTER, "username": "alice_self", "listen_port": 6331})
            _ = recv_msg(s)

            send_msg(s, {"type": ACTION_WEBRTC_OFFER, "target_user": "alice_self", "sdp": "...", "type_name": "offer"})
            resp = recv_msg(s)
            self.assertEqual(resp.get("type"), ACTION_CONNECT_ERROR)
            self.assertIn("Cannot signal oneself", resp.get("error", ""))
        finally:
            s.close()
            time.sleep(0.05)

    def test_user_disconnection_broadcast(self):
        """Test that when a client disconnects, remaining clients are notified."""
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        s1.connect(("127.0.0.1", self.server_port))
        s2.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s1, {"type": ACTION_REGISTER, "username": "staying_user", "listen_port": 6341})
            _ = recv_msg(s1)
            send_msg(s2, {"type": ACTION_REGISTER, "username": "leaving_user", "listen_port": 6342})
            _ = recv_msg(s2)

            # s2 disconnects
            s2.close()
            time.sleep(0.1)

            # s1 receives user_disconnected notice
            notice = recv_msg(s1)
            self.assertEqual(notice.get("type"), ACTION_USER_DISCONNECTED)
            self.assertEqual(notice.get("username"), "leaving_user")

        finally:
            s1.close()
            time.sleep(0.05)

    def test_strict_server_rejection_of_chat_messages(self):
        """
        CRITICAL SECURITY REQUIREMENT:
        The signaling server must NEVER accept, relay, broadcast, or store chat messages.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(s, {"type": ACTION_REGISTER, "username": "test_intruder", "listen_port": 6401})
            _ = recv_msg(s)

            forbidden_payload = {
                "type": ACTION_CHAT_MESSAGE,
                "from": "test_intruder",
                "to": "someone",
                "text": "Secret chat message through server",
            }
            send_msg(s, forbidden_payload)
            resp = recv_msg(s)

            self.assertEqual(resp.get("type"), ACTION_CONNECT_ERROR)
            self.assertIn("Forbidden", resp.get("error", ""))
            self.assertIn("Signaling server handles discovery only", resp.get("error", ""))

        finally:
            s.close()
            time.sleep(0.05)

    def test_re_registration_rejected(self):
        """Test that an active socket cannot re-register under another username."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.server_port))
        try:
            send_msg(s, {"type": ACTION_REGISTER, "username": "user_initial", "listen_port": 6410})
            resp1 = recv_msg(s)
            self.assertEqual(resp1.get("type"), ACTION_REGISTER_ACK)

            # Attempt re-registration on same socket
            send_msg(s, {"type": ACTION_REGISTER, "username": "user_second", "listen_port": 6411})
            resp2 = recv_msg(s)
            self.assertEqual(resp2.get("type"), ACTION_REGISTER_ERROR)
            self.assertIn("already registered", resp2.get("error", ""))
        finally:
            s.close()
            time.sleep(0.05)

    def test_malformed_signaling_payloads(self):
        """Test that malformed WebRTC signaling messages are rejected with clear errors."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.server_port))
        try:
            send_msg(s, {"type": ACTION_REGISTER, "username": "validator_user", "listen_port": 6420})
            _ = recv_msg(s)

            # Missing SDP
            send_msg(s, {"type": ACTION_WEBRTC_OFFER, "target_user": "bob", "type_name": "offer"})
            resp = recv_msg(s)
            self.assertEqual(resp.get("type"), ACTION_CONNECT_ERROR)
            self.assertIn("Missing or invalid SDP", resp.get("error", ""))

            # Invalid target username format
            send_msg(s, {"type": ACTION_WEBRTC_OFFER, "target_user": "invalid username with spaces!", "sdp": "v=0...", "type_name": "offer"})
            resp = recv_msg(s)
            self.assertEqual(resp.get("type"), ACTION_CONNECT_ERROR)
            self.assertIn("Invalid target_user format", resp.get("error", ""))

            # Missing candidate in webrtc_ice
            send_msg(s, {"type": ACTION_WEBRTC_ICE, "target_user": "bob"})
            resp = recv_msg(s)
            self.assertEqual(resp.get("type"), ACTION_CONNECT_ERROR)
            self.assertIn("Missing or invalid candidate", resp.get("error", ""))
        finally:
            s.close()
            time.sleep(0.05)


class TestDirectP2PChatCommunication(unittest.TestCase):
    """
    Test direct WebRTC DataChannel peer-to-peer messaging between clients:
    1. Direct connection establishment via SDP/ICE exchange.
    2. Bidirectional chat messages exclusively over the DataChannel.
    3. Multi-peer connections.
    4. Timeout and failure handling.
    5. Clean closure and reconnection.
    """

    @classmethod
    def setUpClass(cls):
        cls.server_port = get_free_port()
        cls.server = SignalingServer(host="127.0.0.1", port=cls.server_port)
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_direct_p2p_chat_session(self):
        client_alice = PeerClient(
            username="alice_p2p",
            signaling_ip="127.0.0.1",
            signaling_port=self.server_port,
        )

        client_bob = PeerClient(
            username="bob_p2p",
            signaling_ip="127.0.0.1",
            signaling_port=self.server_port,
        )

        try:
            client_alice.start_peer_listener()
            client_bob.start_peer_listener()

            self.assertTrue(client_alice.connect_to_signaling_server())
            self.assertTrue(client_bob.connect_to_signaling_server())

            connected = client_alice.connect_to_peer("bob_p2p")
            self.assertTrue(connected)

            time.sleep(0.2)

            self.assertIn("bob_p2p", client_alice.active_peers)
            self.assertIn("alice_p2p", client_bob.active_peers)
            self.assertEqual(client_alice.active_chat_partner, "bob_p2p")

            # Alice sends message to Bob
            alice_msg_sent = client_alice.send_chat_message("bob_p2p", "Hello from Alice direct P2P!")
            self.assertTrue(alice_msg_sent)

            # Bob sends message to Alice
            bob_msg_sent = client_bob.send_chat_message("alice_p2p", "Hello Alice, loud and clear!")
            self.assertTrue(bob_msg_sent)

            # Graceful closure
            client_alice.close_peer("bob_p2p")
            time.sleep(0.1)
            self.assertNotIn("bob_p2p", client_alice.active_peers)

        finally:
            client_alice.disconnect_all()
            client_bob.disconnect_all()
            time.sleep(0.1)

    def test_multiple_peers_chat(self):
        """Test a client can manage concurrent direct WebRTC DataChannels with multiple peers."""
        client_alice = PeerClient(username="alice_multi", signaling_ip="127.0.0.1", signaling_port=self.server_port)
        client_bob = PeerClient(username="bob_multi", signaling_ip="127.0.0.1", signaling_port=self.server_port)
        client_charlie = PeerClient(username="charlie_multi", signaling_ip="127.0.0.1", signaling_port=self.server_port)

        try:
            self.assertTrue(client_alice.connect_to_signaling_server())
            self.assertTrue(client_bob.connect_to_signaling_server())
            self.assertTrue(client_charlie.connect_to_signaling_server())

            # Alice connects to Bob
            self.assertTrue(client_alice.connect_to_peer("bob_multi"))
            # Alice connects to Charlie
            self.assertTrue(client_alice.connect_to_peer("charlie_multi"))

            time.sleep(0.2)

            # Alice has two active WebRTC peers
            self.assertIn("bob_multi", client_alice.active_peers)
            self.assertIn("charlie_multi", client_alice.active_peers)

            # Alice messages Bob
            self.assertTrue(client_alice.send_chat_message("bob_multi", "Hi Bob!"))
            # Alice messages Charlie
            self.assertTrue(client_alice.send_chat_message("charlie_multi", "Hi Charlie!"))

        finally:
            client_alice.disconnect_all()
            client_bob.disconnect_all()
            client_charlie.disconnect_all()
            time.sleep(0.1)

    def test_duplicate_connection_attempt(self):
        """Test that calling connect_to_peer when already connected returns True immediately."""
        client_alice = PeerClient(username="alice_dup", signaling_ip="127.0.0.1", signaling_port=self.server_port)
        client_bob = PeerClient(username="bob_dup", signaling_ip="127.0.0.1", signaling_port=self.server_port)

        try:
            client_alice.connect_to_signaling_server()
            client_bob.connect_to_signaling_server()

            self.assertTrue(client_alice.connect_to_peer("bob_dup"))
            time.sleep(0.2)

            # Re-attempting connection to already connected peer returns True
            self.assertTrue(client_alice.connect_to_peer("bob_dup"))

        finally:
            client_alice.disconnect_all()
            client_bob.disconnect_all()
            time.sleep(0.1)

    def test_connection_timeout_handling(self):
        """Test that connection attempt to a non-responsive target times out cleanly."""
        client_alice = PeerClient(username="alice_timeout", signaling_ip="127.0.0.1", signaling_port=self.server_port)

        # Connect a dummy socket that registers as 'dummy_peer' but never answers WebRTC offers
        dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        dummy_sock.connect(("127.0.0.1", self.server_port))

        try:
            send_msg(dummy_sock, {"type": ACTION_REGISTER, "username": "dummy_peer", "listen_port": 6999})
            _ = recv_msg(dummy_sock)

            client_alice.connect_to_signaling_server()

            # Attempt connection to non-responsive dummy_peer (will time out)
            connected = client_alice.connect_to_peer("dummy_peer")
            self.assertFalse(connected)
            self.assertNotIn("dummy_peer", client_alice.active_peers)

        finally:
            dummy_sock.close()
            client_alice.disconnect_all()
            time.sleep(0.1)

    def test_peer_cleanup_and_reconnection(self):
        """Test that closing a connection and reconnecting succeeds cleanly."""
        client_alice = PeerClient(username="alice_reconn", signaling_ip="127.0.0.1", signaling_port=self.server_port)
        client_bob = PeerClient(username="bob_reconn", signaling_ip="127.0.0.1", signaling_port=self.server_port)

        try:
            client_alice.connect_to_signaling_server()
            client_bob.connect_to_signaling_server()

            # 1. Connect
            self.assertTrue(client_alice.connect_to_peer("bob_reconn"))
            time.sleep(0.2)
            self.assertIn("bob_reconn", client_alice.active_peers)

            # 2. Close
            client_alice.close_peer("bob_reconn")
            time.sleep(0.2)
            self.assertNotIn("bob_reconn", client_alice.active_peers)

            # 3. Reconnect
            self.assertTrue(client_alice.connect_to_peer("bob_reconn"))
            time.sleep(0.2)
            self.assertIn("bob_reconn", client_alice.active_peers)
            self.assertTrue(client_alice.send_chat_message("bob_reconn", "Reconnected successfully!"))

        finally:
            client_alice.disconnect_all()
            client_bob.disconnect_all()
            time.sleep(0.1)

    def test_immediate_failure_on_offline_peer(self):
        """Test that attempting to connect to an offline peer fails immediately without 10s timeout."""
        client_alice = PeerClient(username="alice_fast_fail", signaling_ip="127.0.0.1", signaling_port=self.server_port)
        try:
            self.assertTrue(client_alice.connect_to_signaling_server())
            t0 = time.time()
            res = client_alice.connect_to_peer("nobody_online")
            elapsed = time.time() - t0

            self.assertFalse(res)
            self.assertLess(elapsed, 2.0, f"Expected fast failure, but took {elapsed:.2f}s")
            self.assertNotIn("nobody_online", client_alice.active_peers)
        finally:
            client_alice.disconnect_all()
            time.sleep(0.1)

    def test_send_oversized_chat_message(self):
        """Test that sending a message larger than MAX_MESSAGE_SIZE is cleanly rejected."""
        client_alice = PeerClient(username="alice_size", signaling_ip="127.0.0.1", signaling_port=self.server_port)
        client_bob = PeerClient(username="bob_size", signaling_ip="127.0.0.1", signaling_port=self.server_port)
        try:
            client_alice.connect_to_signaling_server()
            client_bob.connect_to_signaling_server()
            self.assertTrue(client_alice.connect_to_peer("bob_size"))
            time.sleep(0.2)

            oversized_text = "A" * (MAX_MESSAGE_SIZE + 1000)
            success = client_alice.send_chat_message("bob_size", oversized_text)
            self.assertFalse(success, "Oversized message must be rejected")

            # Normal message should still send fine
            normal_success = client_alice.send_chat_message("bob_size", "Normal message within limits")
            self.assertTrue(normal_success)
        finally:
            client_alice.disconnect_all()
            client_bob.disconnect_all()
            time.sleep(0.1)


class TestRepositoryFilesAndSecurity(unittest.TestCase):
    """Verify GitHub readiness, presence of .env.example with STUN config, and .gitignore rules."""

    def setUp(self):
        self.workspace_root = os.path.dirname(os.path.abspath(__file__))

    def test_env_example_exists(self):
        example_path = os.path.join(self.workspace_root, ".env.example")
        self.assertTrue(os.path.isfile(example_path), ".env.example must exist in repository root")

        with open(example_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("SERVER_HOST", content)
        self.assertIn("SERVER_PORT", content)
        self.assertIn("SIGNALING_SERVER_IP", content)
        self.assertIn("STUN_SERVER_URL", content)
        self.assertIn("LOG_LEVEL", content)

    def test_gitignore_rules(self):
        gitignore_path = os.path.join(self.workspace_root, ".gitignore")
        self.assertTrue(os.path.isfile(gitignore_path), ".gitignore must exist in repository root")

        with open(gitignore_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]

        self.assertIn(".env", lines)
        self.assertIn("!.env.example", lines)
        self.assertIn("__pycache__/", lines)
        self.assertIn(".venv/", lines)


if __name__ == "__main__":
    unittest.main()
