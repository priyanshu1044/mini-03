#!/usr/bin/env python3
"""
Script to start multiple replication servers.
This demonstrates the distributed nature of the system.
"""
import os
import sys
import time
import signal
import argparse
import subprocess
import traceback
from typing import List, Dict

# Get the absolute path to the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

def start_server(server_id: str) -> subprocess.Popen:
    """Start a server process with the given ID."""
    server_script = os.path.join(PROJECT_ROOT, "src", "server", "server.py")
    
    # Create and start the process
    process = subprocess.Popen(
        [sys.executable, server_script, server_id],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1,  # Line buffered
        env=os.environ.copy()  # Pass environment variables
    )
    
    print(f"Started server {server_id} with PID {process.pid}")
    return process

def monitor_output(processes: Dict[str, subprocess.Popen]):
    """Monitor and print output from all processes."""
    try:
        while processes:
            for server_id, process in list(processes.items()):
                # Check if process is still running
                if process.poll() is not None:
                    print(f"Server {server_id} exited with code {process.returncode}")
                    del processes[server_id]
                    continue
                
                # Read output line by line (non-blocking)
                while True:
                    output = process.stdout.readline()
                    if not output:
                        break
                    print(f"[{server_id}] {output.strip()}")
            
            # Sleep to avoid high CPU usage
            time.sleep(0.1)
    
    except KeyboardInterrupt:
        print("Stopping all servers...")
        for server_id, process in processes.items():
            print(f"Terminating server {server_id}...")
            process.terminate()
        
        # Wait for processes to terminate
        for server_id, process in processes.items():
            try:
                process.wait(timeout=5)
                print(f"Server {server_id} terminated successfully")
            except subprocess.TimeoutExpired:
                print(f"Killing server {server_id} forcefully...")
                process.kill()

def main():
    parser = argparse.ArgumentParser(description="Start multiple replication servers")
    parser.add_argument("--servers", type=str, default="server1,server2,server3",
                      help="Comma-separated list of server IDs to start")
    parser.add_argument("--local-only", action="store_true",
                      help="Only start servers configured for the local machine")
    parser.add_argument("--debug", action="store_true",
                      help="Print full error traceback for debugging")
    args = parser.parse_args()
    
    # Get server IDs to start
    if args.local_only:
        # Import server config to get the list of nodes
        sys.path.append(PROJECT_ROOT)
        try:
            from src.server.config import NODES
            
            # Detect local hostname or IP
            import socket
            local_hostname = socket.gethostname()
            local_ip = socket.gethostbyname(local_hostname)
            
            # Get servers configured for this machine
            server_ids = []
            for node in NODES:
                if node["host"] in ("localhost", "127.0.0.1", local_hostname, local_ip):
                    server_ids.append(node["id"])
        except Exception as e:
            print(f"Error determining local servers: {e}")
            if args.debug:
                traceback.print_exc()
            return
    else:
        server_ids = args.servers.split(",")
    
    if not server_ids:
        print("No servers specified. Exiting.")
        return
    
    print(f"Starting {len(server_ids)} servers: {', '.join(server_ids)}")
    
    # Start all servers
    processes = {}
    try:
        for server_id in server_ids:
            processes[server_id] = start_server(server_id)
            # Wait a bit between server starts to avoid race conditions
            time.sleep(1)
        
        # Monitor output from all processes
        monitor_output(processes)
    
    except Exception as e:
        print(f"Error starting servers: {e}")
        if args.debug:
            traceback.print_exc()
    
    finally:
        # Make sure we clean up on exit
        for process in processes.values():
            if process.poll() is None:
                process.terminate()

if __name__ == "__main__":
    main()