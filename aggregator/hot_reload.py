"""
Configuration hot-reload — detect YAML config changes without restart.

Watches validation pipeline config for changes and reloads automatically.

Usage:
    watcher = ConfigWatcher("aggregator/validation/config.yaml")
    watcher.start()  # background thread
    
    # Later:
    if watcher.has_changed():
        pipeline = ValidationPipeline.from_config()  # reload
"""
import os
import time
import hashlib
import logging
import threading
from typing import Optional, Callable

log = logging.getLogger(__name__)


class ConfigWatcher:
    """
    Watches a config file for changes using hash comparison.
    
    Thread-safe, lightweight, no external dependencies.
    """

    def __init__(self, config_path: str, check_interval: int = 30,
                 on_change: Optional[Callable] = None):
        self.config_path = os.path.abspath(config_path)
        self.check_interval = check_interval
        self.on_change = on_change
        self._last_hash = self._compute_hash()
        self._changed = False
        self._lock = threading.Lock()
        self._thread = None
        self._running = False

    def _compute_hash(self) -> str:
        """Compute MD5 hash of config file."""
        try:
            with open(self.config_path, "rb") as f:
                return hashlib.md5(f.read()).hexdigest()
        except FileNotFoundError:
            return ""

    def has_changed(self) -> bool:
        """Check if config has changed since last check."""
        with self._lock:
            current = self._compute_hash()
            if current != self._last_hash:
                self._last_hash = current
                self._changed = True
                return True
            return False

    def start(self):
        """Start background watcher thread."""
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()
        log.info(f"ConfigWatcher started for {self.config_path}")

    def stop(self):
        """Stop background watcher."""
        self._running = False

    def _watch_loop(self):
        """Background loop checking for changes."""
        while self._running:
            if self.has_changed():
                log.info(f"Config change detected: {self.config_path}")
                if self.on_change:
                    try:
                        self.on_change()
                    except Exception as e:
                        log.error(f"Hot-reload callback failed: {e}")
            time.sleep(self.check_interval)

    def reload_if_changed(self) -> bool:
        """Check and reload — returns True if reloaded."""
        if self.has_changed():
            if self.on_change:
                self.on_change()
            return True
        return False
