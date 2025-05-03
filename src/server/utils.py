"""Server utility classes and functions."""
import time
import psutil
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

@dataclass
class SystemMetrics:
    """System metrics snapshot."""
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    queue_size: int = 0
    timestamp: float = 0.0

@dataclass
class MessageData:
    """Message data stored in the system."""
    message_id: str
    content: str
    replicas: List[str]
    timestamp: float = 0.0
    steal_count: int = 0  # Number of times this message has been stolen

class ResourceMonitor:
    """Monitor system resources and manage message queue."""
    
    def __init__(self):
        """Initialize the resource monitor."""
        self.message_queue: Dict[str, MessageData] = {}
        self.metrics = SystemMetrics()
        self.metrics_lock = threading.Lock()
        self.queue_lock = threading.Lock()
        # Initialize with a metrics reading
        self.update_metrics()
    
    def update_metrics(self) -> SystemMetrics:
        """Update and return the current system metrics."""
        with self.metrics_lock:
            self.metrics.cpu_percent = psutil.cpu_percent(interval=0.1)
            self.metrics.memory_percent = psutil.virtual_memory().percent
            self.metrics.queue_size = len(self.message_queue)
            self.metrics.timestamp = time.time()
            return self.metrics
    
    def get_load_score(self) -> float:
        """
        Get a normalized load score between 0.0 and 1.0.
        Higher values indicate higher load.
        """
        # Weight factors for different metrics
        CPU_WEIGHT = 0.5
        MEM_WEIGHT = 0.3
        QUEUE_WEIGHT = 0.2
        
        # Get current metrics
        metrics = self.update_metrics()
        
        # Normalize metrics
        cpu_normalized = min(metrics.cpu_percent / 100.0, 1.0)
        mem_normalized = min(metrics.memory_percent / 100.0, 1.0)
        
        # Normalize queue size - consider 1000 messages as full load
        queue_normalized = min(metrics.queue_size / 1000.0, 1.0)
        
        # Compute weighted average
        load_score = (
            cpu_normalized * CPU_WEIGHT +
            mem_normalized * MEM_WEIGHT +
            queue_normalized * QUEUE_WEIGHT
        )
        
        return min(max(load_score, 0.0), 1.0)  # Ensure it's between 0 and 1
    
    def add_message(self, message_id: str, content: str, replicas: List[str]) -> None:
        """Add a message to the message queue."""
        with self.queue_lock:
            self.message_queue[message_id] = MessageData(
                message_id=message_id,
                content=content,
                replicas=replicas,
                timestamp=time.time()
            )
    
    def get_message(self, message_id: str) -> Optional[MessageData]:
        """Get a message from the queue by ID."""
        with self.queue_lock:
            return self.message_queue.get(message_id)
    
    def should_allow_stealing(self, threshold: float) -> bool:
        """
        Determine if the server should allow messages to be stolen.
        Returns True if the load is above the threshold.
        """
        return self.get_load_score() > threshold
    
    def steal_messages(self, max_count: int) -> List[MessageData]:
        """
        Get up to max_count messages that can be stolen by another node.
        Prioritize messages that have been in the queue longer.
        """
        stolen_messages = []
        
        with self.queue_lock:
            # Sort messages by timestamp (oldest first)
            sorted_msgs = sorted(
                self.message_queue.values(),
                key=lambda m: m.timestamp
            )
            
            # Take up to max_count messages
            candidates = sorted_msgs[:max_count]
            
            # Remove the messages from our queue
            for msg in candidates:
                msg.steal_count += 1
                stolen_messages.append(msg)
                del self.message_queue[msg.message_id]
        
        return stolen_messages