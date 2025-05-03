"""Server configuration."""
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

# Server node configurations
# In a real deployment, these would be distributed across multiple machines
NODES = [
    {"id": "server1", "host": "localhost", "port": 50051},
    {"id": "server2", "host": "localhost", "port": 50052},
    {"id": "server3", "host": "localhost", "port": 50053},
    {"id": "server4", "host": "localhost", "port": 50054},
    {"id": "server5", "host": "localhost", "port": 50055},
    # Configuration for the second computer
    {"id": "server6", "host": "10.0.0.223", "port": 50051},
    {"id": "server7", "host": "10.0.0.223", "port": 50052},
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