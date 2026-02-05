"""
Human-like timing for web scraping.

Implements variable delays to mimic human browsing patterns.
"""

import os
import random
import time
import logging

logger = logging.getLogger(__name__)


class HumanTiming:
    """
    Manages human-like delays between requests.
    
    Features:
    - Variable base delays
    - Occasional "reading" pauses
    - Random break intervals
    - Configurable via environment variables
    """
    
    def __init__(self):
        # Load config from environment with defaults
        self.min_delay = float(os.environ.get("MIN_DELAY_SECONDS", 5))
        self.max_delay = float(os.environ.get("MAX_DELAY_SECONDS", 20))
        self.reading_pause_chance = float(os.environ.get("READING_PAUSE_CHANCE", 0.12))
        self.reading_pause_min = float(os.environ.get("READING_PAUSE_MIN", 30))
        self.reading_pause_max = float(os.environ.get("READING_PAUSE_MAX", 90))
        self.break_min = int(os.environ.get("BREAK_AFTER_REQUESTS_MIN", 22))
        self.break_max = int(os.environ.get("BREAK_AFTER_REQUESTS_MAX", 38))
        self.break_duration_min = float(os.environ.get("BREAK_DURATION_MIN", 90))
        self.break_duration_max = float(os.environ.get("BREAK_DURATION_MAX", 180))
        
        self.request_count = 0
        self._next_break_at = self._random_break_threshold()
    
    def _random_break_threshold(self) -> int:
        """Generate a random threshold for the next break."""
        return random.randint(self.break_min, self.break_max)
    
    def delay(self):
        """
        Wait with human-like timing.
        
        Includes random base delay and occasional "reading" pauses.
        """
        # Base delay with variance
        delay = random.uniform(self.min_delay, self.max_delay)
        
        # Occasionally simulate reading (longer pause)
        if random.random() < self.reading_pause_chance:
            delay = random.uniform(self.reading_pause_min, self.reading_pause_max)
            logger.info(f"Simulating reading pause ({delay:.0f}s)...")
        else:
            logger.debug(f"Waiting {delay:.1f}s...")
        
        time.sleep(delay)
    
    def maybe_break(self):
        """
        Take a break if threshold reached.
        
        Breaks occur at random intervals, not fixed counts.
        """
        self.request_count += 1
        
        if self.request_count >= self._next_break_at:
            duration = random.uniform(self.break_duration_min, self.break_duration_max)
            logger.info(
                f"Taking a {duration/60:.1f} minute break "
                f"after {self.request_count} requests..."
            )
            time.sleep(duration)
            
            # Set next break at random interval from now
            self._next_break_at = self.request_count + self._random_break_threshold()
    
    def wait(self):
        """Combined delay and break check."""
        self.delay()
        self.maybe_break()
    
    def reset(self):
        """Reset counters (e.g., for new session)."""
        self.request_count = 0
        self._next_break_at = self._random_break_threshold()
