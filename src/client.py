"""Client script to test the replication system."""
import os
import sys
import time
import uuid
import random
import logging
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor

import grpc

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the generated protocol buffer code
from src.replication_pb2 import (
    StoreRequest, GetRequest, MetricsRequest
)
from src.replication_pb2_grpc import ReplicationServiceStub
from server.config import NODES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ReplicationClient:
    """Client for interacting with the replication system."""
    
    def __init__(self, host='localhost', port=50051):
        """Initialize the client."""
        self.host = host
        self.port = port
        self.connect()
    
    def connect(self):
        """Connect to the server."""
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.stub = ReplicationServiceStub(self.channel)
        logger.info(f"Connected to server at {self.host}:{self.port}")
    
    def store_message(self, message_id=None, content="", replica_servers=None):
        """Store a message in the replication system."""
        if message_id is None:
            message_id = str(uuid.uuid4())
        
        request = StoreRequest(
            message_id=message_id,
            content=content,
            replica_servers=replica_servers or []
        )
        
        try:
            response = self.stub.StoreMessage(request)
            return response
        except Exception as e:
            logger.error(f"Error storing message: {str(e)}")
            return None
    
    def get_message(self, message_id):
        """Retrieve a message from the replication system."""
        request = GetRequest(message_id=message_id)
        
        try:
            response = self.stub.GetMessage(request)
            return response
        except Exception as e:
            logger.error(f"Error retrieving message: {str(e)}")
            return None
    
    def get_metrics(self):
        """Get system metrics from a server."""
        request = MetricsRequest()
        
        try:
            response = self.stub.GetMetrics(request)
            return response
        except Exception as e:
            logger.error(f"Error getting metrics: {str(e)}")
            return None

def run_test_workload(client, num_messages=10, message_size=1024, read_ratio=0.7):
    """Run a test workload against the system."""
    messages = {}  # message_id -> content
    
    # Generate random data of specified size
    def generate_content(size):
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(size))
    
    # Store phase
    logger.info(f"Starting write phase - storing {num_messages} messages")
    for i in range(num_messages):
        message_id = f"test-msg-{i}-{uuid.uuid4()}"
        content = generate_content(message_size)
        messages[message_id] = content
        
        response = client.store_message(message_id=message_id, content=content)
        if response and response.success:
            logger.info(f"Message {message_id} stored successfully on nodes: {response.stored_on}")
        else:
            logger.error(f"Failed to store message {message_id}")
    
    # Read phase
    logger.info(f"Starting read phase with read ratio {read_ratio}")
    message_ids = list(messages.keys())
    num_reads = int(num_messages * (1/read_ratio))
    
    successful_reads = 0
    failed_reads = 0
    
    for _ in range(num_reads):
        message_id = random.choice(message_ids)
        original_content = messages[message_id]
        
        response = client.get_message(message_id)
        if response and response.found and response.content == original_content:
            successful_reads += 1
            logger.info(f"Message {message_id} retrieved successfully from nodes: {response.replica_locations}")
        else:
            failed_reads += 1
            logger.error(f"Failed to retrieve message {message_id} or content mismatch")
    
    # Print summary
    logger.info(f"Test completed: {successful_reads} successful reads, {failed_reads} failed reads")
    logger.info(f"Read success rate: {successful_reads / max(1, num_reads) * 100:.2f}%")
    
    # Get metrics
    metrics = client.get_metrics()
    if metrics:
        logger.info(f"Server metrics: CPU={metrics.cpu_percent}%, Memory={metrics.memory_percent}%, "
                  f"Queue size={metrics.queue_size}, Load score={metrics.load_score}")

def main():
    parser = argparse.ArgumentParser(description='Replication system client')
    parser.add_argument('--host', type=str, default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=50051, help='Server port')
    parser.add_argument('--messages', type=int, default=10, help='Number of messages to store')
    parser.add_argument('--size', type=int, default=1024, help='Size of each message in bytes')
    parser.add_argument('--read-ratio', type=float, default=0.7, 
                      help='Ratio of reads to writes (higher means more reads)')
    parser.add_argument('--mode', choices=['interactive', 'test'], default='test',
                      help='Run in interactive or test mode')
    
    args = parser.parse_args()
    
    client = ReplicationClient(host=args.host, port=args.port)
    
    if args.mode == 'test':
        run_test_workload(
            client, 
            num_messages=args.messages, 
            message_size=args.size,
            read_ratio=args.read_ratio
        )
    else:
        # Interactive mode
        print("Replication System Interactive Client")
        print("Commands:")
        print("  store <content> - Store a new message")
        print("  get <message_id> - Retrieve a message")
        print("  metrics - Get server metrics")
        print("  exit - Exit the client")
        
        while True:
            try:
                cmd = input("> ").strip().split(maxsplit=1)
                if not cmd:
                    continue
                
                if cmd[0] == "exit":
                    break
                elif cmd[0] == "store":
                    if len(cmd) < 2:
                        print("Usage: store <content>")
                        continue
                    
                    response = client.store_message(content=cmd[1])
                    if response and response.success:
                        print(f"Message stored successfully: {response.message}")
                        print(f"Stored on nodes: {response.stored_on}")
                    else:
                        print("Failed to store message")
                
                elif cmd[0] == "get":
                    if len(cmd) < 2:
                        print("Usage: get <message_id>")
                        continue
                    
                    response = client.get_message(cmd[1])
                    if response and response.found:
                        print(f"Message content: {response.content}")
                        print(f"Found on nodes: {response.replica_locations}")
                    else:
                        print("Message not found")
                
                elif cmd[0] == "metrics":
                    metrics = client.get_metrics()
                    if metrics:
                        print(f"CPU: {metrics.cpu_percent}%")
                        print(f"Memory: {metrics.memory_percent}%")
                        print(f"Queue size: {metrics.queue_size}")
                        print(f"Load score: {metrics.load_score}")
                    else:
                        print("Failed to get metrics")
                
                else:
                    print(f"Unknown command: {cmd[0]}")
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()