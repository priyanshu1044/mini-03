#!/usr/bin/env python3
"""
Load testing script for the replication system.
This script simulates various load patterns to demonstrate
how the NWR algorithm adapts to changing conditions.
"""
import os
import sys
import time
import random
import asyncio
import argparse
import threading
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Get the absolute path to the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from src.client import ReplicationClient
from src.server.config import NODES

# Test types
TEST_TYPES = {
    "write_heavy": {"read_ratio": 0.3, "message_size": 2048},
    "read_heavy": {"read_ratio": 0.8, "message_size": 1024},
    "balanced": {"read_ratio": 0.5, "message_size": 1024},
    "large_msgs": {"read_ratio": 0.5, "message_size": 10240},
    "small_msgs": {"read_ratio": 0.5, "message_size": 128},
    "bursty": {"read_ratio": 0.5, "message_size": 1024, "burst": True},
}

class LoadTest:
    """Load test orchestrator for the replication system."""
    
    def __init__(self, clients: List[ReplicationClient], test_config: Dict[str, Any]):
        """Initialize load test with client connections and configuration."""
        self.clients = clients
        self.config = test_config
        self.is_running = False
        self.message_ids = []  # Stored message IDs for reads
        self.results = {
            "start_time": 0,
            "end_time": 0,
            "total_writes": 0,
            "successful_writes": 0,
            "total_reads": 0,
            "successful_reads": 0,
            "latencies": {
                "write": [],
                "read": []
            }
        }
    
    def _generate_content(self, size: int) -> str:
        """Generate random content of specified size."""
        return ''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(size))
    
    def _write_message(self) -> bool:
        """Write a message to the system and measure latency."""
        client = random.choice(self.clients)
        message_id = f"test-{time.time()}-{random.randint(1000, 9999)}"
        content = self._generate_content(self.config.get("message_size", 1024))
        
        start_time = time.time()
        response = client.store_message(message_id=message_id, content=content)
        end_time = time.time()
        
        latency = end_time - start_time
        self.results["latencies"]["write"].append(latency)
        
        success = response and response.success
        if success:
            self.message_ids.append(message_id)
            print(f"Write success: msg_id={message_id[:10]}..., latency={latency:.3f}s")
        else:
            print(f"Write failed: msg_id={message_id[:10]}..., latency={latency:.3f}s")
        
        return success
    
    def _read_message(self) -> bool:
        """Read a message from the system and measure latency."""
        if not self.message_ids:
            # No messages to read yet
            return False
        
        client = random.choice(self.clients)
        message_id = random.choice(self.message_ids)
        
        start_time = time.time()
        response = client.get_message(message_id)
        end_time = time.time()
        
        latency = end_time - start_time
        self.results["latencies"]["read"].append(latency)
        
        success = response and response.found
        if success:
            print(f"Read success: msg_id={message_id[:10]}..., latency={latency:.3f}s")
        else:
            print(f"Read failed: msg_id={message_id[:10]}..., latency={latency:.3f}s")
        
        return success
    
    def _get_metrics(self) -> Dict[str, Any]:
        """Get system metrics from all servers."""
        metrics = {}
        for i, client in enumerate(self.clients):
            response = client.get_metrics()
            if response:
                metrics[f"client_{i}"] = {
                    "cpu_percent": response.cpu_percent,
                    "memory_percent": response.memory_percent,
                    "queue_size": response.queue_size,
                    "load_score": response.load_score,
                }
        return metrics
    
    def run_worker(self, duration: int, read_ratio: float, max_qps: int = 10, worker_id: int = 0):
        """Worker thread to generate load at a specific rate."""
        sleep_time = 1.0 / max_qps
        end_time = time.time() + duration
        
        print(f"Worker {worker_id} starting - target QPS: {max_qps}, read ratio: {read_ratio}")
        
        while time.time() < end_time and self.is_running:
            op_start = time.time()
            
            # Decide whether to read or write
            if random.random() < read_ratio and self.message_ids:
                # Read operation
                self.results["total_reads"] += 1
                if self._read_message():
                    self.results["successful_reads"] += 1
            else:
                # Write operation
                self.results["total_writes"] += 1
                if self._write_message():
                    self.results["successful_writes"] += 1
            
            # Sleep to maintain QPS target
            op_duration = time.time() - op_start
            if op_duration < sleep_time:
                time.sleep(sleep_time - op_duration)
    
    def run_test(self, duration: int, num_workers: int = 4):
        """Run a load test for the specified duration."""
        self.is_running = True
        self.results["start_time"] = time.time()
        
        read_ratio = self.config.get("read_ratio", 0.5)
        burst = self.config.get("burst", False)
        
        # Start worker threads
        workers = []
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            for i in range(num_workers):
                # If bursty, vary the QPS for different workers
                if burst:
                    qps = random.choice([5, 10, 20, 30])
                else:
                    qps = 10
                
                future = executor.submit(
                    self.run_worker, 
                    duration, 
                    read_ratio,
                    qps,
                    i
                )
                workers.append(future)
            
            # Start metrics collection thread
            metrics_thread = threading.Thread(
                target=self._collect_metrics, 
                args=(duration,),
                daemon=True
            )
            metrics_thread.start()
            
            # Wait for all workers to complete
            for future in as_completed(workers):
                try:
                    future.result()
                except Exception as e:
                    print(f"Worker error: {str(e)}")
        
        self.is_running = False
        self.results["end_time"] = time.time()
        
        # Wait for metrics thread to complete
        metrics_thread.join()
        
        # Print results
        self._print_results()
    
    def _collect_metrics(self, duration: int):
        """Collect system metrics periodically."""
        end_time = time.time() + duration
        metrics_interval = 5  # seconds
        
        while time.time() < end_time and self.is_running:
            metrics = self._get_metrics()
            print("\n--- System Metrics ---")
            for server, server_metrics in metrics.items():
                print(f"{server}: CPU={server_metrics['cpu_percent']:.1f}%, "
                     f"Memory={server_metrics['memory_percent']:.1f}%, "
                     f"Queue={server_metrics['queue_size']}, "
                     f"Load={server_metrics['load_score']:.2f}")
            print("---------------------\n")
            
            time.sleep(metrics_interval)
    
    def _print_results(self):
        """Print the test results."""
        test_duration = self.results["end_time"] - self.results["start_time"]
        total_ops = self.results["total_reads"] + self.results["total_writes"]
        successful_ops = self.results["successful_reads"] + self.results["successful_writes"]
        
        write_latencies = self.results["latencies"]["write"]
        read_latencies = self.results["latencies"]["read"]
        
        avg_write_latency = sum(write_latencies) / max(len(write_latencies), 1)
        avg_read_latency = sum(read_latencies) / max(len(read_latencies), 1)
        
        print("\n===== Load Test Results =====")
        print(f"Test Duration: {test_duration:.2f} seconds")
        print(f"Total Operations: {total_ops} ({total_ops/test_duration:.2f} ops/sec)")
        print(f"Success Rate: {successful_ops/max(total_ops, 1)*100:.2f}%")
        print(f"Writes: {self.results['total_writes']} attempts, {self.results['successful_writes']} successful ({self.results['successful_writes']/max(self.results['total_writes'], 1)*100:.2f}%)")
        print(f"Reads: {self.results['total_reads']} attempts, {self.results['successful_reads']} successful ({self.results['successful_reads']/max(self.results['total_reads'], 1)*100:.2f}%)")
        print(f"Average Write Latency: {avg_write_latency*1000:.2f} ms")
        print(f"Average Read Latency: {avg_read_latency*1000:.2f} ms")
        print("=============================")

def main():
    parser = argparse.ArgumentParser(description="Load testing tool for replication system")
    parser.add_argument("--servers", type=str, default="127.0.0.1:50051,127.0.0.1:50052",
                      help="Comma-separated list of server addresses in host:port format")
    parser.add_argument("--duration", type=int, default=60,
                      help="Test duration in seconds")
    parser.add_argument("--workers", type=int, default=4,
                      help="Number of worker threads")
    parser.add_argument("--test-type", choices=TEST_TYPES.keys(), default="balanced",
                      help="Type of test to run")
    
    args = parser.parse_args()
    
    # Create clients
    clients = []
    server_addresses = args.servers.split(",")
    for address in server_addresses:
        try:
            host, port = address.split(":")
            clients.append(ReplicationClient(host=host, port=int(port)))
        except Exception as e:
            print(f"Error connecting to {address}: {str(e)}")
    
    if not clients:
        print("No clients could be created. Exiting.")
        return
    
    # Get test configuration
    test_config = TEST_TYPES[args.test_type]
    print(f"Running {args.test_type} test with {args.workers} workers for {args.duration} seconds")
    print(f"Test parameters: {test_config}")
    
    # Create and run the load test
    load_test = LoadTest(clients, test_config)
    load_test.run_test(args.duration, args.workers)

if __name__ == "__main__":
    main()