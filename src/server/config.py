"""Server configuration."""
import os
import socket
from dataclasses import dataclass
from typing import Dict, List, Optional

# Detect local IP address to help with cross-machine communication
def get_local_ip():
    try:
        # This creates a socket that doesn't actually connect but helps us determine the IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Use a public DNS server to determine what our IP would be (doesn't actually send data)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception:
        # Fallback to localhost if we can't determine the IP
        return "127.0.0.1"

# Get the local IP address
LOCAL_IP = get_local_ip()
# Define the remote IP address
REMOTE_IP = "10.0.0.223"

# Server node configurations
# In a real deployment, these would be distributed across multiple machines
NODES = [
    {"id": "server1", "host": LOCAL_IP, "port": 50051},
    {"id": "server2", "host": LOCAL_IP, "port": 50052},
    {"id": "server3", "host": LOCAL_IP, "port": 50053},
    {"id": "server4", "host": LOCAL_IP, "port": 50054},
    {"id": "server5", "host": LOCAL_IP, "port": 50055},
    # Configuration for the second computer
    {"id": "server6", "host": REMOTE_IP, "port": 50051},
    {"id": "server7", "host": REMOTE_IP, "port": 50052},
]

@dataclass
class ServerConfig:
    """Configuration for a server node."""
    id: str
    host: str
    port: int
    min_replicas: int = 3  # N parameter - default number of replicas
    min_writes: int = 2    # W parameter - default write quorum
    min_reads: int = 2     # R parameter - default read quorum
    steal_threshold: float = 0.7  # Load threshold for allowing message stealing
    heartbeat_interval: float = 2.0  # Seconds between heartbeats

def get_server_config(server_id: str) -> ServerConfig:
    """Get the configuration for a specific server."""
    for node in NODES:
        if node["id"] == server_id:
            return ServerConfig(
                id=server_id,
                host=node["host"],
                port=node["port"],
                # Read overrides from environment variables if present
                min_replicas=int(os.environ.get("MIN_REPLICAS", 3)),
                min_writes=int(os.environ.get("MIN_WRITES", 2)),
                min_reads=int(os.environ.get("MIN_READS", 2)),
                steal_threshold=float(os.environ.get("STEAL_THRESHOLD", 0.7)),
                heartbeat_interval=float(os.environ.get("HEARTBEAT_INTERVAL", 2.0))
            )
    
    raise ValueError(f"No configuration found for server {server_id}")