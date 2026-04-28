#!/usr/bin/env python3
"""
Connection Scheduler for Round-Robin Stream Monitoring.

Coordinates FFmpeg processes to fetch segments one at a time,
allowing smooth monitoring when provider limits concurrent connections.

Architecture:
- Round-robin scheduling: Each stream gets a turn to fetch a segment
- Burst mode: Streams in "review" status get multiple consecutive turns
  for proper loop detection
- Semaphore-like acquire/release mechanism

Example (5 streams, 3s segments):
  Time 0-3s:  Stream 1 fetches segment
  Time 3-6s:  Stream 2 fetches segment
  Time 6-9s:  Stream 3 fetches segment
  Time 9-12s: Stream 4 fetches segment
  Time 12-15s: Stream 5 fetches segment
  Time 15-18s: Stream 1 fetches next segment (repeat)

Burst mode for review streams:
  Stream in review gets 3 consecutive segments (9s continuous) for loop detection
"""

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Callable
from enum import Enum

from apps.core.logging_config import setup_logging

logger = setup_logging(__name__)


class StreamPriority(Enum):
    """Priority levels for stream scheduling."""
    STABLE = 1      # Normal round-robin
    REVIEW = 2      # Burst mode for loop detection
    VIEWER = 3      # User is watching - prioritize


@dataclass
class ScheduledStream:
    """Represents a stream in the scheduler queue."""
    stream_id: int
    session_id: str
    priority: StreamPriority
    last_fetch_time: float = 0.0
    burst_count: int = 0           # How many consecutive fetches remaining
    burst_target: int = 1          # Target burst count (1 for stable, 3 for review)
    viewer_count: int = 0          # Number of active viewers
    
    def needs_burst(self) -> bool:
        """Check if this stream needs burst mode."""
        return self.burst_count > 0 or self.priority == StreamPriority.REVIEW
    
    def start_burst(self, target: int = 3):
        """Start burst mode for loop detection."""
        self.burst_target = target
        self.burst_count = target
    
    def end_burst(self):
        """End burst mode."""
        self.burst_count = 0
        self.burst_target = 1


class ConnectionScheduler:
    """
    Manages round-robin scheduling for stream segment fetching.
    
    Ensures only ONE stream is actively fetching from the provider at any moment,
    solving the single-connection limit issue.
    
    Features:
    - Round-robin for stable streams (smooth monitoring for all)
    - Burst mode for review streams (proper loop detection)
    - Viewer priority (streams being watched get priority)
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        
        # Registered streams by session
        self.streams: Dict[str, Dict[int, ScheduledStream]] = {}
        
        # Current active stream (the one allowed to fetch)
        self.active_stream: Optional[ScheduledStream] = None
        self.active_session_id: Optional[str] = None
        self.active_stream_id: Optional[int] = None
        
        # Queue for round-robin ordering
        self.queue: deque = deque()
        
        # Lock for thread safety
        self._state_lock = threading.Lock()
        
        # Condition variable for waiting streams
        self._condition = threading.Condition(self._state_lock)
        
        # Stats
        self.total_fetches = 0
        self.burst_fetches = 0
        
        # Configuration
        self.segment_duration = 3.0          # Expected segment duration (seconds)
        self.review_burst_count = 3          # Consecutive segments for review streams
        self.viewer_burst_count = 2          # Extra segments when viewer is watching
        self.max_wait_time = 30.0            # Max time to wait for turn
        
        logger.info("ConnectionScheduler initialized for round-robin monitoring")
    
    def register_stream(self, session_id: str, stream_id: int, status: str = 'stable'):
        """
        Register a stream for scheduling.
        
        Args:
            session_id: Session ID
            stream_id: Stream ID
            status: Stream status ('stable', 'review', 'quarantined')
        """
        with self._state_lock:
            if session_id not in self.streams:
                self.streams[session_id] = {}
            
            # Determine priority
            if status == 'review':
                priority = StreamPriority.REVIEW
                burst_target = self.review_burst_count
            elif status == 'stable':
                priority = StreamPriority.STABLE
                burst_target = 1
            else:
                # Quarantined streams not scheduled
                return
            
            scheduled = ScheduledStream(
                stream_id=stream_id,
                session_id=session_id,
                priority=priority,
                burst_target=burst_target
            )
            
            self.streams[session_id][stream_id] = scheduled
            
            # Add to queue if not already there
            key = (session_id, stream_id)
            if key not in self.queue:
                self.queue.append(key)
            
            logger.debug(f"Registered stream {stream_id} (session {session_id}) with priority {priority.name}")
    
    def unregister_stream(self, session_id: str, stream_id: int):
        """
        Unregister a stream from scheduling.
        
        Args:
            session_id: Session ID
            stream_id: Stream ID
        """
        with self._state_lock:
            if session_id in self.streams:
                if stream_id in self.streams[session_id]:
                    del self.streams[session_id][stream_id]
                    
                    # Remove from queue
                    key = (session_id, stream_id)
                    try:
                        self.queue.remove(key)
                    except ValueError:
                        pass
                    
                    # Clear active if this was the active stream
                    if self.active_stream_id == stream_id and self.active_session_id == session_id:
                        self.active_stream = None
                        self.active_session_id = None
                        self.active_stream_id = None
                        self._condition.notify_all()
                    
                    logger.debug(f"Unregistered stream {stream_id} (session {session_id})")
    
    def update_stream_status(self, session_id: str, stream_id: int, status: str):
        """
        Update a stream's status (and priority).
        
        Args:
            session_id: Session ID
            stream_id: Stream ID
            status: New status ('stable', 'review', 'quarantined')
        """
        with self._state_lock:
            if session_id in self.streams and stream_id in self.streams[session_id]:
                scheduled = self.streams[session_id][stream_id]
                
                if status == 'review':
                    scheduled.priority = StreamPriority.REVIEW
                    scheduled.start_burst(self.review_burst_count)
                    logger.info(f"Stream {stream_id} moved to REVIEW - starting burst mode ({self.review_burst_count} segments)")
                    
                elif status == 'stable':
                    scheduled.priority = StreamPriority.STABLE
                    scheduled.end_burst()
                    logger.debug(f"Stream {stream_id} moved to STABLE - normal round-robin")
                    
                elif status == 'quarantined':
                    self.unregister_stream(session_id, stream_id)
    
    def add_viewer(self, session_id: str, stream_id: int):
        """
        Mark that a viewer is watching this stream (boost priority).
        
        Args:
            session_id: Session ID
            stream_id: Stream ID
        """
        with self._state_lock:
            if session_id in self.streams and stream_id in self.streams[session_id]:
                scheduled = self.streams[session_id][stream_id]
                scheduled.viewer_count += 1
                scheduled.priority = StreamPriority.VIEWER
                
                # Give extra burst segments for smooth viewing
                if scheduled.burst_target < self.viewer_burst_count:
                    scheduled.burst_target = self.viewer_burst_count
                    scheduled.burst_count = self.viewer_burst_count
                
                logger.debug(f"Viewer added to stream {stream_id} - priority boosted")
    
    def remove_viewer(self, session_id: str, stream_id: int):
        """
        Mark that a viewer stopped watching this stream.
        
        Args:
            session_id: Session ID
            stream_id: Stream ID
        """
        with self._state_lock:
            if session_id in self.streams and stream_id in self.streams[session_id]:
                scheduled = self.streams[session_id][stream_id]
                scheduled.viewer_count -= 1
                
                if scheduled.viewer_count <= 0:
                    scheduled.viewer_count = 0
                    # Reset priority based on status
                    if scheduled.burst_target == self.viewer_burst_count:
                        scheduled.burst_target = 1
                        scheduled.burst_count = 0
                    
                    logger.debug(f"Viewer removed from stream {stream_id}")
    
    def acquire_fetch_slot(self, session_id: str, stream_id: int, timeout: float = None) -> bool:
        """
        Acquire permission to fetch a segment.
        
        Blocks until it's this stream's turn in the round-robin cycle.
        
        Args:
            session_id: Session ID
            stream_id: Stream ID
            timeout: Maximum time to wait (default: self.max_wait_time)
            
        Returns:
            True if slot acquired, False if timeout or stream not registered
        """
        if timeout is None:
            timeout = self.max_wait_time
        
        start_time = time.time()
        
        with self._condition:
            # Check if stream is registered
            if session_id not in self.streams or stream_id not in self.streams[session_id]:
                logger.warning(f"Stream {stream_id} not registered in scheduler")
                return False
            
            scheduled = self.streams[session_id][stream_id]
            
            # Wait until it's our turn
            while True:
                # Check if we're already the active stream (burst mode continuation)
                if self.active_stream_id == stream_id and self.active_session_id == session_id:
                    if scheduled.burst_count > 0:
                        scheduled.burst_count -= 1
                        self.total_fetches += 1
                        self.burst_fetches += 1
                        logger.debug(f"Stream {stream_id} continuing burst ({scheduled.burst_count} remaining)")
                        return True
                
                # Check timeout
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    logger.warning(f"Stream {stream_id} timed out waiting for fetch slot ({elapsed:.1f}s)")
                    return False
                
                # If no active stream, we can become active
                if self.active_stream is None:
                    self._set_active(session_id, stream_id)
                    return True
                
                # Check if we're next in queue
                if len(self.queue) > 0:
                    next_key = self.queue[0]
                    if next_key == (session_id, stream_id):
                        self._set_active(session_id, stream_id)
                        return True
                
                # Need to wait - rotate queue to find next candidate
                self._rotate_queue()
                
                # Wait for active stream to release
                self._condition.wait(timeout=min(1.0, timeout - elapsed))
    
    def release_fetch_slot(self, session_id: str, stream_id: int):
        """
        Release the fetch slot after segment is complete.
        
        Args:
            session_id: Session ID
            stream_id: Stream ID
        """
        with self._condition:
            # Verify we're the active stream
            if self.active_stream_id != stream_id or self.active_session_id != session_id:
                logger.warning(f"Stream {stream_id} tried to release slot it doesn't hold")
                return
            
            scheduled = self.streams[session_id][stream_id]
            scheduled.last_fetch_time = time.time()
            
            # Check if burst continues
            if scheduled.burst_count > 0:
                # Don't release - stream continues burst
                logger.debug(f"Stream {stream_id} burst continues ({scheduled.burst_count} remaining)")
                self._condition.notify_all()
                return
            
            # End of turn - rotate queue and notify next stream
            self._rotate_queue()
            self.active_stream = None
            self.active_session_id = None
            self.active_stream_id = None
            
            logger.debug(f"Stream {stream_id} released fetch slot")
            self._condition.notify_all()
    
    def _set_active(self, session_id: str, stream_id: int):
        """Set the given stream as active."""
        scheduled = self.streams[session_id][stream_id]
        self.active_stream = scheduled
        self.active_session_id = session_id
        self.active_stream_id = stream_id
        
        # Initialize burst if needed
        if scheduled.needs_burst() and scheduled.burst_count == 0:
            scheduled.start_burst(scheduled.burst_target)
        
        # Decrement burst count for this fetch
        if scheduled.burst_count > 0:
            scheduled.burst_count -= 1
        
        self.total_fetches += 1
        
        # Remove from front of queue and add to back (will be added back after release)
        key = (session_id, stream_id)
        try:
            self.queue.remove(key)
        except ValueError:
            pass
        
        logger.debug(f"Stream {stream_id} acquired fetch slot (burst: {scheduled.burst_count} remaining)")
    
    def _rotate_queue(self):
        """Rotate queue to prioritize viewer/review streams."""
        if len(self.queue) <= 1:
            return
        
        # Build prioritized queue
        prioritized = []
        normal = []
        
        for key in self.queue:
            session_id, stream_id = key
            if session_id in self.streams and stream_id in self.streams[session_id]:
                scheduled = self.streams[session_id][stream_id]
                if scheduled.priority == StreamPriority.VIEWER:
                    prioritized.append(key)
                elif scheduled.priority == StreamPriority.REVIEW:
                    prioritized.append(key)  # Also high priority
                else:
                    normal.append(key)
        
        # Rebuild queue: prioritized first, then normal round-robin
        self.queue = deque(prioritized + normal)
    
    def get_stats(self) -> dict:
        """Get scheduler statistics."""
        with self._state_lock:
            return {
                'registered_streams': sum(len(s) for s in self.streams.values()),
                'active_stream': self.active_stream_id,
                'queue_length': len(self.queue),
                'total_fetches': self.total_fetches,
                'burst_fetches': self.burst_fetches,
            }
    
    def clear_session(self, session_id: str):
        """Clear all streams for a session."""
        with self._state_lock:
            if session_id in self.streams:
                for stream_id in list(self.streams[session_id].keys()):
                    self.unregister_stream(session_id, stream_id)
                del self.streams[session_id]
                logger.info(f"Cleared all streams for session {session_id}")


# Global accessor
def get_connection_scheduler() -> ConnectionScheduler:
    """Get the global ConnectionScheduler instance."""
    return ConnectionScheduler()