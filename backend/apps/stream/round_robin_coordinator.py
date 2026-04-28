#!/usr/bin/env python3
"""
Round-Robin Coordinator for Stream Monitoring.

Works alongside the ConnectionScheduler to coordinate which streams
have active FFmpeg monitors at any time.

For providers with single-connection limits, this ensures:
1. Only one stream is actively monitored (fetching data)
2. All streams get fair time slices in round-robin order
3. Review streams get burst mode for loop detection
4. Viewer-prioritized streams get extra time

This replaces the "fighting" behavior where all FFmpeg processes
try to connect simultaneously.
"""

import threading
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

from apps.core.logging_config import setup_logging
from apps.stream.connection_scheduler import (
    get_connection_scheduler,
    StreamPriority,
)

logger = setup_logging(__name__)


@dataclass
class CoordinatorSlot:
    """Represents a monitoring slot for a stream."""
    session_id: str
    stream_id: int
    acquired: bool = False
    acquire_time: float = 0.0
    release_time: float = 0.0


class RoundRobinCoordinator:
    """
    Coordinates FFmpeg monitor processes for round-robin scheduling.
    
    Integration points:
    1. Before starting a monitor, stream acquires a slot
    2. Monitor runs for slot duration (fetches segments)
    3. Monitor stops/restarts when slot is released
    4. Next stream acquires slot
    
    For continuous monitoring:
    - Stable streams: 1 segment (~3s) per cycle
    - Review streams: 3 segments (~9s) for loop detection
    - Viewer streams: 2 segments (~6s) for smooth playback
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
        
        # Reference to the connection scheduler
        self.scheduler = get_connection_scheduler()
        
        # Active slots
        self.slots: Dict[str, CoordinatorSlot] = {}  # key: f"{session_id}:{stream_id}"
        
        # Monitor references (set by monitoring service)
        self.monitors: Dict[str, Dict[int, any]] = {}  # session_id -> stream_id -> monitor
        
        # Lock
        self._state_lock = threading.Lock()
        
        # Coordinator thread
        self._coordinator_thread: Optional[threading.Thread] = None
        self._running = False
        
        # Configuration
        self.slot_duration = 3.0           # Duration per slot (one segment)
        self.coordination_interval = 0.5   # How often to check coordination
        
        logger.info("RoundRobinCoordinator initialized")
    
    def start(self):
        """Start the coordinator thread."""
        if self._running:
            return
        
        self._running = True
        self._coordinator_thread = threading.Thread(
            target=self._coordinator_loop,
            daemon=True,
            name="RoundRobinCoordinator"
        )
        self._coordinator_thread.start()
        logger.info("RoundRobinCoordinator started")
    
    def stop(self):
        """Stop the coordinator."""
        self._running = False
        if self._coordinator_thread:
            self._coordinator_thread.join(timeout=2.0)
        logger.info("RoundRobinCoordinator stopped")
    
    def register_session_streams(self, session_id: str, stream_statuses: Dict[int, str]):
        """
        Register all streams for a session with their statuses.
        
        Args:
            session_id: Session ID
            stream_statuses: Dict of stream_id -> status ('stable', 'review', etc.)
        """
        for stream_id, status in stream_statuses.items():
            if status != 'quarantined':
                self.scheduler.register_stream(session_id, stream_id, status)
    
    def unregister_session(self, session_id: str):
        """Unregister all streams for a session."""
        self.scheduler.clear_session(session_id)
        
        with self._state_lock:
            # Clear slots for this session
            keys_to_remove = [k for k in self.slots if k.startswith(f"{session_id}:")]
            for key in keys_to_remove:
                del self.slots[key]
    
    def set_monitors_ref(self, session_id: str, monitors: Dict[int, any]):
        """Set reference to monitors dict for a session."""
        self.monitors[session_id] = monitors
    
    def request_slot(self, session_id: str, stream_id: int, timeout: float = 5.0) -> bool:
        """
        Request a monitoring slot for a stream.
        
        This should be called before starting/restarting a monitor.
        
        Args:
            session_id: Session ID
            stream_id: Stream ID
            timeout: Max time to wait for slot
            
        Returns:
            True if slot acquired, False otherwise
        """
        key = f"{session_id}:{stream_id}"
        
        acquired = self.scheduler.acquire_fetch_slot(session_id, stream_id, timeout)
        
        if acquired:
            with self._state_lock:
                self.slots[key] = CoordinatorSlot(
                    session_id=session_id,
                    stream_id=stream_id,
                    acquired=True,
                    acquire_time=time.time()
                )
            logger.debug(f"Slot acquired for stream {stream_id}")
        
        return acquired
    
    def release_slot(self, session_id: str, stream_id: int):
        """
        Release a monitoring slot.
        
        This should be called after a monitor completes its segment fetch.
        """
        key = f"{session_id}:{stream_id}"
        
        with self._state_lock:
            if key in self.slots:
                self.slots[key].acquired = False
                self.slots[key].release_time = time.time()
        
        self.scheduler.release_fetch_slot(session_id, stream_id)
        logger.debug(f"Slot released for stream {stream_id}")
    
    def update_stream_status(self, session_id: str, stream_id: int, status: str):
        """Update a stream's status in the scheduler."""
        self.scheduler.update_stream_status(session_id, stream_id, status)
    
    def add_viewer(self, session_id: str, stream_id: int):
        """Mark a stream as being viewed."""
        self.scheduler.add_viewer(session_id, stream_id)
    
    def remove_viewer(self, session_id: str, stream_id: int):
        """Mark a stream as no longer being viewed."""
        self.scheduler.remove_viewer(session_id, stream_id)
    
    def _coordinator_loop(self):
        """Main coordination loop - monitors slot durations and rotates."""
        while self._running:
            try:
                self._check_slot_expirations()
                time.sleep(self.coordination_interval)
            except Exception as e:
                logger.error(f"Coordinator loop error: {e}")
                time.sleep(1.0)
    
    def _check_slot_expirations(self):
        """Check if any slots have expired and should be rotated."""
        current_time = time.time()
        
        with self._state_lock:
            for key, slot in list(self.slots.items()):
                if slot.acquired:
                    elapsed = current_time - slot.acquire_time
                    
                    # Check if slot duration exceeded
                    if elapsed >= self.slot_duration:
                        # Get scheduled stream info
                        session_id, stream_id = slot.session_id, slot.stream_id
                        
                        # Check burst mode
                        if session_id in self.scheduler.streams:
                            if stream_id in self.scheduler.streams[session_id]:
                                scheduled = self.scheduler.streams[session_id][stream_id]
                                
                                # If in burst mode, allow more time
                                if scheduled.burst_count > 0:
                                    # Continue burst
                                    continue
                        
                        # Slot expired - release it
                        logger.debug(f"Slot expired for stream {stream_id} after {elapsed:.1f}s")
                        # Release happens outside lock to avoid deadlock
                        self._release_slot_async(slot.session_id, slot.stream_id)
    
    def _release_slot_async(self, session_id: str, stream_id: int):
        """Release slot asynchronously (called from within lock)."""
        # This is called from within _state_lock, so we need to be careful
        # The actual release happens in the scheduler which has its own lock
        try:
            self.release_slot(session_id, stream_id)
        except Exception as e:
            logger.error(f"Error releasing slot: {e}")
    
    def get_active_slot(self) -> Optional[CoordinatorSlot]:
        """Get the currently active slot."""
        with self._state_lock:
            for slot in self.slots.values():
                if slot.acquired:
                    return slot
        return None
    
    def is_stream_active(self, session_id: str, stream_id: int) -> bool:
        """Check if a specific stream currently has an active slot."""
        key = f"{session_id}:{stream_id}"
        with self._state_lock:
            if key in self.slots:
                return self.slots[key].acquired
        return False
    
    def get_stats(self) -> dict:
        """Get coordinator statistics."""
        scheduler_stats = self.scheduler.get_stats()
        
        with self._state_lock:
            active_slots = sum(1 for s in self.slots.values() if s.acquired)
            
        return {
            **scheduler_stats,
            'active_slots': active_slots,
            'total_slots': len(self.slots),
        }


# Global accessor
def get_round_robin_coordinator() -> RoundRobinCoordinator:
    """Get the global RoundRobinCoordinator instance."""
    return RoundRobinCoordinator()