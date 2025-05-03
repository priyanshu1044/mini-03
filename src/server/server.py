"""Replication server implementation."""
import os
import sys
import time
import uuid
import random
import logging
import threading
from concurrent import futures
from typing import Dict, List, Optional, Tuple

import grpc
import psutil

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.config import ServerConfig, get_server_config, NODES
from server.utils import ResourceMonitor, MessageData, SystemMetrics

# Import the generated protocol buffer code
from src.replication_pb2 import (
    HeartbeatRequest, HeartbeatResponse,
    StoreRequest, StoreResponse,
    GetRequest, GetResponse,
    StealRequest, StealResponse,
    MetricsRequest, MetricsResponse,
    Message
)
from src.replication_pb2_grpc import (
    ReplicationServiceServicer,
    add_ReplicationServiceServicer_to_server
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ReplicationServer(ReplicationServiceServicer):
    """
    Implementation of the ReplicationService service.
    Handles message replication, load balancing, and NWR parameter management.
    """
    def __init__(self, server_id: str):
        """Initialize the replication server."""
        self.server_id = server_id
        self.config = get_server_config(server_id)
        self.resource_monitor = ResourceMonitor()
        self.node_statuses = {}  # Keep track of node health and load
        self.message_channels = {}  # Channels to other nodes
        self.connections_lock = threading.Lock()
        self.active = True
        
        # Dynamic NWR parameters
        self.min_replicas = self.config.min_replicas  # N
        self.min_writes = self.config.min_writes      # W
        self.min_reads = self.config.min_reads        # R
        
        # Start background monitoring
        self._start_monitoring()
        logger.info(f"Server {self.server_id} initialized with NWR: {self.min_replicas}, {self.min_writes}, {self.min_reads}")

    def _start_monitoring(self):
        """Start background monitoring threads."""
        self.heartbeat_thread = threading.Thread(
            target=self._send_heartbeats,
            daemon=True
        )
        self.heartbeat_thread.start()
        
        self.adaptive_nwr_thread = threading.Thread(
            target=self._adjust_nwr_parameters,
            daemon=True
        )
        self.adaptive_nwr_thread.start()
        
        logger.info(f"Server {self.server_id} monitoring threads started")

    def _connect_to_node(self, node_id: str) -> grpc.Channel:
        """Create or get an existing connection to another node."""
        with self.connections_lock:
            if node_id in self.message_channels:
                return self.message_channels[node_id]
            
            for node in NODES:
                if node["id"] == node_id:
                    channel = grpc.insecure_channel(f"{node['host']}:{node['port']}")
                    self.message_channels[node_id] = channel
                    return channel
                    
            raise ValueError(f"No configuration found for node {node_id}")

    def _send_heartbeats(self):
        """Periodically send heartbeats to other nodes."""
        while self.active:
            # Update metrics first
            metrics = self.resource_monitor.update_metrics()
            load_score = self.resource_monitor.get_load_score()
            
            # Prepare heartbeat request
            request = HeartbeatRequest(
                server_id=self.server_id,
                load_score=load_score,
                queue_size=metrics.queue_size,
                cpu_usage=metrics.cpu_percent,
                memory_usage=metrics.memory_percent
            )
            
            # Send heartbeats to all nodes
            for node in NODES:
                node_id = node["id"]
                if node_id != self.server_id:
                    try:
                        channel = self._connect_to_node(node_id)
                        stub = src.replication_pb2_grpc.ReplicationServiceStub(channel)
                        response = stub.Heartbeat(request, timeout=1.0)
                        
                        # Update node status
                        self.node_statuses[node_id] = {
                            "is_alive": response.is_alive,
                            "load_score": response.load_score,
                            "last_seen": time.time()
                        }
                        
                    except Exception as e:
                        # Mark node as offline
                        self.node_statuses[node_id] = {
                            "is_alive": False,
                            "load_score": 1.0,  # High load means don't use it
                            "last_seen": time.time(),
                            "error": str(e)
                        }
                        logger.warning(f"Failed to connect to node {node_id}: {str(e)}")
            
            # Sleep before next heartbeat
            time.sleep(self.config.heartbeat_interval)

    def _adjust_nwr_parameters(self):
        """Periodically adjust NWR parameters based on network and load conditions."""
        while self.active:
            try:
                # Check how many nodes are alive
                alive_nodes = sum(1 for status in self.node_statuses.values() 
                                 if status.get("is_alive", False))
                total_nodes = len(NODES)
                
                # Get current system load
                load_score = self.resource_monitor.get_load_score()
                
                # Adjust N based on available nodes
                new_n = min(max(2, alive_nodes), total_nodes)
                
                # Adjust W and R based on load
                # Higher load -> lower W, higher R to prefer reads
                # Lower load -> higher W, lower R to ensure consistency
                if load_score > 0.7:  # High load
                    new_w = max(1, min(new_n - 1, 2))  # Reduce writes
                    new_r = max(1, min(new_n, 2))      # Maintain reads
                elif load_score < 0.3:  # Low load
                    new_w = max(2, min(new_n, 3))      # Increase writes
                    new_r = max(1, min(new_n - 1, 2))  # Reduce reads
                else:  # Normal load
                    new_w = max(2, min(new_n - 1, 3))
                    new_r = max(2, min(new_n - 1, 3))
                
                # Ensure W + R > N for consistency
                if new_w + new_r <= new_n:
                    new_r = max(1, new_n - new_w + 1)
                
                # Update NWR parameters
                self.min_replicas = new_n
                self.min_writes = new_w
                self.min_reads = new_r
                
                logger.info(f"Adjusted NWR parameters: N={new_n}, W={new_w}, R={new_r}, "
                           f"Load={load_score:.2f}, Alive nodes={alive_nodes}/{total_nodes}")
            
            except Exception as e:
                logger.error(f"Error adjusting NWR parameters: {e}")
            
            # Check every 5 seconds
            time.sleep(5)

    def _select_replica_nodes(self, message_id: str) -> List[str]:
        """
        Select nodes for replication based on the hash of the message ID.
        Uses consistent hashing and node load to select replica nodes.
        """
        available_nodes = []
        
        # First, get a list of available nodes sorted by load
        for node in NODES:
            node_id = node["id"]
            
            # Skip ourselves
            if node_id == self.server_id:
                continue
                
            # Check if the node is alive
            status = self.node_statuses.get(node_id, {"is_alive": False, "load_score": 1.0})
            if status.get("is_alive", False):
                available_nodes.append((node_id, status.get("load_score", 1.0)))
        
        # Sort by load (lowest first)
        available_nodes.sort(key=lambda x: x[1])
        
        # If we have fewer than N-1 nodes available, use what we have
        needed_replicas = min(self.min_replicas - 1, len(available_nodes))
        
        # Pick the nodes with the lowest load
        selected_nodes = [node_id for node_id, _ in available_nodes[:needed_replicas]]
        
        # Add ourselves to the replica list
        selected_nodes.append(self.server_id)
        
        return selected_nodes

    def Heartbeat(self, request, context):
        """
        Handle heartbeat requests from other nodes.
        Returns the server's health status and load information.
        """
        metrics = self.resource_monitor.update_metrics()
        load_score = self.resource_monitor.get_load_score()
        
        return HeartbeatResponse(
            is_alive=True,
            server_id=self.server_id,
            load_score=load_score
        )

    def StoreMessage(self, request, context):
        """
        Store a message and replicate it to other nodes.
        Implements the NWR algorithm for writes.
        """
        message_id = request.message_id
        content = request.content
        replica_servers = list(request.replica_servers)
        
        logger.info(f"Received store request for message {message_id}, "
                  f"replicas: {replica_servers}")
        
        # If no replica servers specified, select them based on load and availability
        if not replica_servers:
            replica_servers = self._select_replica_nodes(message_id)
        
        # Add message to our local store
        self.resource_monitor.add_message(message_id, content, replica_servers)
        
        # Track successful writes
        successful_writes = 1  # Count ourselves
        stored_on = [self.server_id]
        
        # Replicate to other nodes
        for node_id in replica_servers:
            if node_id != self.server_id:
                try:
                    channel = self._connect_to_node(node_id)
                    stub = src.replication_pb2_grpc.ReplicationServiceStub(channel)
                    
                    # Send the same replication request to other nodes
                    store_request = StoreRequest(
                        message_id=message_id,
                        content=content,
                        replica_servers=replica_servers
                    )
                    
                    response = stub.StoreMessage(store_request, timeout=2.0)
                    if response.success:
                        successful_writes += 1
                        stored_on.extend(response.stored_on)
                except Exception as e:
                    logger.error(f"Failed to replicate message to {node_id}: {e}")
        
        # Check if we achieved the minimum write goal
        success = successful_writes >= self.min_writes
        message = (f"Message stored on {successful_writes}/{self.min_writes} "
                 f"required nodes")
        
        return StoreResponse(
            success=success,
            message=message,
            stored_on=stored_on
        )

    def GetMessage(self, request, context):
        """
        Retrieve a message from the distributed system.
        Implements the NWR algorithm for reads.
        """
        message_id = request.message_id
        logger.info(f"Received get request for message {message_id}")
        
        # Check if we have the message locally
        message = self.resource_monitor.message_queue.get(message_id)
        
        if message:
            # We found it locally, check if we need to talk to other replicas
            replicas = message.replicas
            local_content = message.content
            
            # If we have fewer replicas than R, contact other nodes
            if len(replicas) < self.min_reads:
                successful_reads = 1  # Count ourselves
                all_replicas = [self.server_id]
                
                # Try to read from other replicas
                for node_id in replicas:
                    if node_id != self.server_id:
                        try:
                            channel = self._connect_to_node(node_id)
                            stub = src.replication_pb2_grpc.ReplicationServiceStub(channel)
                            
                            response = stub.GetMessage(GetRequest(message_id=message_id), 
                                                    timeout=2.0)
                            
                            if response.found:
                                successful_reads += 1
                                all_replicas.extend(response.replica_locations)
                        except Exception as e:
                            logger.error(f"Failed to read message from {node_id}: {e}")
                
                return GetResponse(
                    found=successful_reads >= self.min_reads,
                    content=local_content if successful_reads >= self.min_reads else "",
                    replica_locations=all_replicas
                )
            else:
                # We have enough replicas locally
                return GetResponse(
                    found=True,
                    content=local_content,
                    replica_locations=[self.server_id]
                )
        else:
            # We don't have it locally, check other nodes
            for node in NODES:
                node_id = node["id"]
                if node_id != self.server_id:
                    try:
                        channel = self._connect_to_node(node_id)
                        stub = src.replication_pb2_grpc.ReplicationServiceStub(channel)
                        
                        response = stub.GetMessage(GetRequest(message_id=message_id), 
                                                timeout=2.0)
                        
                        if response.found:
                            return response
                    except Exception as e:
                        logger.error(f"Failed to check message on {node_id}: {e}")
            
            # Not found anywhere
            return GetResponse(
                found=False,
                content="",
                replica_locations=[]
            )

    def RequestMessageSteal(self, request, context):
        """
        Handle requests from other nodes to steal messages to balance load.
        """
        requesting_server = request.requesting_server
        max_messages = request.max_messages
        
        # Check if we have enough load to allow stealing
        if not self.resource_monitor.should_allow_stealing(self.config.steal_threshold):
            return StealResponse(stolen_messages=[])
        
        # Get messages that can be stolen
        stolen_messages = self.resource_monitor.steal_messages(max_messages)
        
        # Convert to protobuf messages
        proto_messages = []
        for msg in stolen_messages:
            proto_messages.append(Message(
                message_id=msg.message_id,
                content=msg.content,
                replicas=msg.replicas,
                steal_count=msg.steal_count
            ))
        
        logger.info(f"Allowing server {requesting_server} to steal {len(proto_messages)} messages")
        
        return StealResponse(stolen_messages=proto_messages)

    def GetMetrics(self, request, context):
        """
        Return current system metrics for monitoring.
        """
        metrics = self.resource_monitor.update_metrics()
        load_score = self.resource_monitor.get_load_score()
        
        return MetricsResponse(
            cpu_percent=metrics.cpu_percent,
            memory_percent=metrics.memory_percent,
            queue_size=metrics.queue_size,
            load_score=load_score
        )

def serve(server_id: str, max_workers: int = 10):
    """Start the replication server."""
    server_config = get_server_config(server_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    
    servicer = ReplicationServer(server_id)
    add_ReplicationServiceServicer_to_server(servicer, server)
    
    server_address = f"{server_config.host}:{server_config.port}"
    server.add_insecure_port(server_address)
    server.start()
    
    logger.info(f"Server {server_id} started on {server_address}")
    
    try:
        # Keep the server running until interrupted
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"Server {server_id} stopping...")
        servicer.active = False
        server.stop(0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <server_id>")
        sys.exit(1)
    
    server_id = sys.argv[1]
    serve(server_id)