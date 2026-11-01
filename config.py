"""
config.py - Centralized configuration, environment loader, and network utilities.

Supports standard-library .env loading without external dependencies,
environment-variable overrides, validation for IPs and ports, and LAN IP discovery.
"""

import os
import socket
import logging
import ipaddress
from typing import Dict, Any, Optional, List

logger = logging.getLogger("p2p_chat.config")


def load_dotenv(filepath: str = ".env") -> Dict[str, str]:
    """
    Load environment variables from a .env file into os.environ.
    Existing os.environ variables take precedence over .env file entries.
    Does not require external dependencies.
    """
    loaded = {}
    if not os.path.isfile(filepath):
        return loaded

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip()
                    # Strip wrapping quotes if present
                    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                        val = val[1:-1]
                    # Only set if not already set in environment
                    if key not in os.environ:
                        os.environ[key] = val
                    loaded[key] = val
    except Exception as e:
        logger.warning("Could not read .env file at %s: %s", filepath, e)

    return loaded


def get_lan_ip() -> str:
    """
    Attempt to determine the primary local LAN IPv4 address of this machine.
    Uses a UDP probe to a non-routable/external address without sending actual packets.
    Falls back to 127.0.0.1 if network is unavailable.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 10.255.255.255:1 is a dummy target that forces the OS routing table
        # to select the active outbound LAN interface. No packet is actually sent.
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def parse_bool(value: Optional[str], default: bool = False) -> bool:
    """Parse string representation of boolean."""
    if value is None:
        return default
    val = value.strip().lower()
    if val in ("true", "1", "yes", "t", "y", "on"):
        return True
    if val in ("false", "0", "no", "f", "n", "off"):
        return False
    return default


def validate_port(port: Any, name: str = "Port") -> int:
    """Validate that port is an integer between 1024 and 65535."""
    try:
        port_num = int(port)
        if not (1024 <= port_num <= 65535):
            raise ValueError(f"{name} must be between 1024 and 65535, got {port_num}")
        return port_num
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid {name}: {e}") from e


def validate_ip(ip: str, name: str = "IP Address", allow_all_interfaces: bool = True) -> str:
    """
    Validate that an IP string is a strict, valid IPv4 address.
    If allow_all_interfaces is True, '0.0.0.0' is permitted (useful for listening bind).
    """
    ip = ip.strip()
    if ip == "0.0.0.0":
        if allow_all_interfaces:
            return ip
        raise ValueError(f"{name} cannot be 0.0.0.0 in this context")

    try:
        # Require exactly 4 decimal octets
        parts = ip.split(".")
        if len(parts) != 4:
            raise ValueError(f"{name} must contain exactly 4 octets: {ip}")
        # Parse strictly as IPv4 address
        ip_obj = ipaddress.IPv4Address(ip)
        return str(ip_obj)
    except Exception as e:
        raise ValueError(f"Invalid {name} '{ip}': {e}") from e



def validate_host(host: str, name: str = "Host") -> str:
    """
    Validate that a host string is a valid IPv4 address, IPv6 address, or hostname/FQDN.
    """
    host = host.strip()
    if not host:
        raise ValueError(f"{name} cannot be empty")
    try:
        ipaddress.ip_address(host)
        return host
    except ValueError:
        pass
    if len(host) > 253:
        raise ValueError(f"{name} is too long: {host}")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._")
    if not set(host).issubset(allowed):
        raise ValueError(f"Invalid characters in {name}: {host}")
    return host


def validate_stun_urls(raw_urls: Optional[str], default: str = "stun:stun.l.google.com:19302") -> List[str]:
    """
    Parse and validate comma-separated STUN server URLs.
    Ensures every URL starts with 'stun:' or 'stuns:'.
    """
    target = (raw_urls or "").strip()
    if not target:
        target = default

    parsed_urls = []
    for item in target.split(","):
        url = item.strip()
        if not url:
            continue
        if not (url.startswith("stun:") or url.startswith("stuns:")):
            raise ValueError(f"Invalid STUN URL '{url}'. Must begin with 'stun:' or 'stuns:'.")
        parsed_urls.append(url)

    if not parsed_urls:
        raise ValueError("At least one valid STUN URL must be configured.")
    return parsed_urls


class AppConfig:
    """Application configuration holder with environment loading and validation."""

    def __init__(self, env_file: str = ".env"):
        # Load local .env if present
        load_dotenv(env_file)

        # Logging configuration
        self.log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
        self.debug_mode = parse_bool(os.getenv("DEBUG_MODE", "false"), default=False)
        self.log_level = logging.DEBUG if self.debug_mode else getattr(logging, self.log_level_str, logging.INFO)

        # Signaling server configuration
        self.server_host = validate_ip(os.getenv("SERVER_HOST", "0.0.0.0"), "SERVER_HOST", allow_all_interfaces=True)
        self.server_port = validate_port(os.getenv("SERVER_PORT", "5050"), "SERVER_PORT")

        # Client connection configuration
        # For client: signaling server IP or hostname to connect to
        raw_sig_ip = os.getenv("SIGNALING_SERVER_IP", "127.0.0.1").strip()
        self.signaling_server_ip = validate_host(raw_sig_ip, "SIGNALING_SERVER_IP")

        # WebRTC STUN Server configuration (used for ICE candidate discovery across NAT)
        raw_stun = os.getenv("STUN_SERVER_URL", "stun:stun.l.google.com:19302")
        self.stun_server_urls = validate_stun_urls(raw_stun)
        self.stun_server_url = self.stun_server_urls[0] if self.stun_server_urls else "stun:stun.l.google.com:19302"

        # Client P2P listen host and port (retained for backward compatibility / local binding)
        self.client_listen_host = validate_ip(
            os.getenv("CLIENT_LISTEN_HOST", "0.0.0.0"), "CLIENT_LISTEN_HOST", allow_all_interfaces=True
        )
        self.client_listen_port = validate_port(os.getenv("CLIENT_LISTEN_PORT", "6001"), "CLIENT_LISTEN_PORT")

        # Optional client username preset
        self.client_username = os.getenv("CLIENT_USERNAME", "").strip()

    def configure_logging(self, logger_name: Optional[str] = None):
        """Configure standard logging format across components."""
        logging.basicConfig(
            level=self.log_level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        if logger_name:
            return logging.getLogger(logger_name)
        return logging.getLogger("p2p_chat")
