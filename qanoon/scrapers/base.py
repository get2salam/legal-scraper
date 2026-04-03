"""
Base Scraper Class
==================

Common functionality for all PLS scrapers:
- curl_cffi session with Chrome 120 TLS fingerprint
- Login flow with CSRF token handling
- Human-like delays with jitter
- Operating hours enforcement
- Rate limiting with exponential backoff
- Break simulation
- Request retries

This is the foundation that CaseScraper, LegislationScraper, and
LinkedCasesScraper all inherit from.
"""

import os
import re
import time
import random
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Any
from abc import ABC, abstractmethod

from curl_cffi.requests import Session, BrowserType
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://www.pakistanlawsite.com"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing (human-like)
DEFAULT_MIN_DELAY = 3.0
DEFAULT_MAX_DELAY = 8.0
LOGIN_DELAY = 5.0
RATE_LIMIT_BACKOFF = 60
READING_DELAY_MIN = 2.0
READING_DELAY_MAX = 6.0

# Break simulation
REQUESTS_BEFORE_BREAK = 30
BREAK_MIN = 30
BREAK_MAX = 90

# Pakistan timezone offset
PKT_OFFSET = timedelta(hours=5)

# Logging
logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Base class for all PLS scrapers.
    
    Provides:
    - Session management with Chrome TLS fingerprint
    - Login/logout with CSRF handling
    - Human-like delays with jitter
    - Operating hours enforcement
    - Rate limiting and exponential backoff
    - Random breaks to simulate human behavior
    - Request retry logic
    
    Subclasses must implement:
    - scrape(): Main scraping logic
    
    Args:
        ignore_hours: Skip operating hours check (for testing)
        use_proxy: Use Bright Data proxy (requires credentials)
        open_hour: Hour when PLS opens (PKT timezone)
        close_hour: Hour when PLS closes (PKT timezone)
        night_mode: If True, operating window crosses midnight
    """
    
    # Default operating hours (can be overridden by subclasses)
    DEFAULT_OPEN_HOUR = 7   # 7 AM PKT
    DEFAULT_CLOSE_HOUR = 21  # 9 PM PKT
    DEFAULT_NIGHT_MODE = False
    
    def __init__(
        self,
        ignore_hours: bool = False,
        use_proxy: bool = False,
        open_hour: int = None,
        close_hour: int = None,
        night_mode: bool = None,
        min_delay: float = None,
        max_delay: float = None,
    ):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.requests_since_break = 0
        
        # Configuration
        self.ignore_hours = ignore_hours
        self.use_proxy = use_proxy
        self.open_hour = open_hour if open_hour is not None else self.DEFAULT_OPEN_HOUR
        self.close_hour = close_hour if close_hour is not None else self.DEFAULT_CLOSE_HOUR
        self.night_mode = night_mode if night_mode is not None else self.DEFAULT_NIGHT_MODE
        self.min_delay = min_delay if min_delay is not None else DEFAULT_MIN_DELAY
        self.max_delay = max_delay if max_delay is not None else DEFAULT_MAX_DELAY
        
        # Proxy configuration
        self.proxy_url = None
        if self.use_proxy:
            self._setup_proxy()
    
    def _setup_proxy(self):
        """Configure Bright Data proxy for Pakistan IPs."""
        bd_username = os.getenv("BRIGHTDATA_USERNAME", "")
        bd_password = os.getenv("BRIGHTDATA_PASSWORD", "")
        bd_host = os.getenv("BRIGHTDATA_HOST", "brd.superproxy.io")
        bd_port = os.getenv("BRIGHTDATA_PORT", "33335")
        
        if bd_username and bd_password:
            self.proxy_url = f"http://{bd_username}-country-pk:{bd_password}@{bd_host}:{bd_port}"
            logger.info(f"Using Bright Data Pakistan proxy ({bd_host}:{bd_port})")
        else:
            logger.warning("No proxy credentials found, running direct")
            self.use_proxy = False
    
    def _create_session(self) -> Session:
        """Create a curl_cffi session with Chrome 120 TLS fingerprint."""
        session = Session(impersonate=BrowserType.chrome120)
        
        # Set proxy if configured
        if self.proxy_url:
            session.proxies = {
                "http": self.proxy_url,
                "https": self.proxy_url,
            }
            session.verify = False
        
        # Chrome-like headers
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        })
        
        return session
    
    # ══════════════════════════════════════════════════════════════════════════
    # Operating Hours
    # ══════════════════════════════════════════════════════════════════════════
    
    def is_open(self) -> bool:
        """Check if PLS is within operating hours."""
        if self.ignore_hours:
            return True
        
        utc_now = datetime.now(timezone.utc)
        pkt_now = utc_now + PKT_OFFSET
        current_hour = pkt_now.hour
        
        if self.night_mode:
            # Night shift crosses midnight (e.g., 22:00 - 05:00)
            is_open = current_hour >= self.open_hour or current_hour < self.close_hour
        else:
            # Day shift (e.g., 07:00 - 21:00)
            is_open = self.open_hour <= current_hour < self.close_hour
        
        if not is_open:
            logger.info(
                f"Outside operating hours (PKT: {pkt_now.strftime('%H:%M')}). "
                f"Window: {self.open_hour}:00-{self.close_hour}:00 PKT"
            )
        
        return is_open
    
    def wait_for_open(self) -> None:
        """Wait until operating window opens."""
        while not self.is_open():
            utc_now = datetime.now(timezone.utc)
            pkt_now = utc_now + PKT_OFFSET
            
            if self.night_mode:
                # Night mode: wait until open_hour today
                if pkt_now.hour >= self.close_hour and pkt_now.hour < self.open_hour:
                    open_time = datetime(
                        pkt_now.year, pkt_now.month, pkt_now.day, 
                        self.open_hour, 0
                    )
                else:
                    open_time = datetime(
                        pkt_now.year, pkt_now.month, pkt_now.day, 
                        self.open_hour, 0
                    )
            else:
                # Day mode
                if pkt_now.hour >= self.close_hour:
                    tomorrow = pkt_now.date() + timedelta(days=1)
                    open_time = datetime(
                        tomorrow.year, tomorrow.month, tomorrow.day, 
                        self.open_hour, 0
                    )
                else:
                    open_time = datetime(
                        pkt_now.year, pkt_now.month, pkt_now.day, 
                        self.open_hour, 0
                    )
            
            wait_seconds = (open_time - pkt_now.replace(tzinfo=None)).total_seconds()
            wait_seconds = max(60, wait_seconds)
            
            hours, remainder = divmod(int(wait_seconds), 3600)
            minutes = remainder // 60
            
            logger.info(f"Waiting {hours}h {minutes}m until window opens at {self.open_hour}:00 PKT...")
            
            # Sleep in 5-minute chunks to allow interruption
            chunk = min(300, wait_seconds)
            time.sleep(chunk)
    
    # ══════════════════════════════════════════════════════════════════════════
    # Delays & Rate Limiting
    # ══════════════════════════════════════════════════════════════════════════
    
    def human_delay(
        self, 
        min_s: float = None, 
        max_s: float = None, 
        reading: bool = False
    ):
        """
        Wait a random human-like delay with Gaussian jitter.
        
        Args:
            min_s: Minimum delay seconds
            max_s: Maximum delay seconds
            reading: If True, use longer "reading" delays
        """
        if reading:
            min_s = min_s or READING_DELAY_MIN
            max_s = max_s or READING_DELAY_MAX
        else:
            min_s = min_s or self.min_delay
            max_s = max_s or self.max_delay
        
        delay = random.uniform(min_s, max_s)
        # Add Gaussian jitter for natural timing
        delay += random.gauss(0, 0.5)
        delay = max(1.0, delay)
        
        logger.debug(f"Waiting {delay:.1f}s...")
        time.sleep(delay)
    
    def maybe_take_break(self) -> None:
        """Take a random break every N requests to simulate human behavior."""
        self.requests_since_break += 1
        
        if self.requests_since_break >= REQUESTS_BEFORE_BREAK:
            break_duration = random.uniform(BREAK_MIN, BREAK_MAX)
            logger.info(f"Taking a {break_duration:.0f}s break (human simulation)...")
            time.sleep(break_duration)
            self.requests_since_break = 0
    
    # ══════════════════════════════════════════════════════════════════════════
    # HTTP Requests
    # ══════════════════════════════════════════════════════════════════════════
    
    def request(
        self, 
        method: str, 
        url: str, 
        retries: int = 3, 
        **kwargs
    ) -> Optional[Any]:
        """
        Make an HTTP request with rate limiting, retries, and error handling.
        
        Args:
            method: HTTP method (GET, POST)
            url: Request URL
            retries: Number of retry attempts
            **kwargs: Additional arguments passed to curl_cffi
            
        Returns:
            Response object or None on failure
        """
        # Check operating hours
        if not self.is_open():
            self.wait_for_open()
            self.logged_in = False
            if not self.login():
                return None
        
        # Maybe take a break
        self.maybe_take_break()
        
        # Enforce minimum delay
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        
        last_error = None
        for attempt in range(retries):
            try:
                if method.upper() == "GET":
                    resp = self.session.get(url, timeout=30, **kwargs)
                else:
                    resp = self.session.post(url, timeout=30, **kwargs)
                
                self.last_request_time = time.time()
                self.request_count += 1
                
                # Handle rate limiting
                if resp.status_code == 403:
                    backoff = RATE_LIMIT_BACKOFF * (attempt + 1)
                    logger.warning(f"403 Forbidden - backing off {backoff}s (attempt {attempt + 1}/{retries})...")
                    time.sleep(backoff)
                    continue
                
                if resp.status_code == 429:
                    backoff = RATE_LIMIT_BACKOFF * (2 ** attempt)
                    logger.warning(f"429 Rate Limited - backing off {backoff}s (attempt {attempt + 1}/{retries})...")
                    time.sleep(backoff)
                    continue
                
                if resp.status_code == 500:
                    logger.warning(f"500 Server Error for {url} (attempt {attempt + 1}/{retries})")
                    time.sleep(RATE_LIMIT_BACKOFF)
                    continue
                
                if resp.status_code != 200:
                    logger.warning(f"Unexpected status {resp.status_code} for {url}")
                    return None
                
                return resp
                
            except Exception as e:
                last_error = e
                backoff = RATE_LIMIT_BACKOFF * (attempt + 1)
                logger.error(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(backoff)
        
        logger.error(f"All {retries} attempts failed for {url}: {last_error}")
        return None
    
    # ══════════════════════════════════════════════════════════════════════════
    # Authentication
    # ══════════════════════════════════════════════════════════════════════════
    
    def login(self) -> bool:
        """
        Login to Pakistan Law Site.
        
        Returns:
            True if login successful, False otherwise
        """
        if not self.is_open():
            self.wait_for_open()
        
        logger.info("Logging in to PLS...")
        
        self.session = self._create_session()
        
        # Get homepage for CSRF token
        try:
            resp = self.session.get(f"{BASE_URL}/", timeout=30)
            if resp.status_code != 200:
                logger.error(f"Failed to load homepage: status {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"Failed to load homepage: {e}")
            return False
        
        # Simulate reading the homepage
        self.human_delay(reading=True)
        
        # Extract CSRF token
        csrf_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            resp.text
        )
        if not csrf_match:
            logger.error("CSRF token not found")
            return False
        
        csrf_token = csrf_match.group(1)
        logger.debug(f"CSRF token: {csrf_token[:40]}...")
        
        self.human_delay(2, 4)
        
        # Submit login form
        try:
            login_resp = self.session.post(
                f"{BASE_URL}/Login/Login",
                data={
                    "Login.UserName": PLS_USER,
                    "Login.Password": PLS_PASS,
                    "__RequestVerificationToken": csrf_token
                },
                timeout=30
            )
        except Exception as e:
            logger.error(f"Login request failed: {e}")
            return False
        
        self.human_delay(2, 3)
        
        # Verify login by checking session
        try:
            check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
            if not check_resp or "Logout" not in check_resp.text:
                logger.error("Login verification failed")
                return False
        except Exception as e:
            logger.error(f"Login verification failed: {e}")
            return False
        
        self.logged_in = True
        self.requests_since_break = 0
        logger.info("✓ Login successful!")
        
        self.human_delay(LOGIN_DELAY, LOGIN_DELAY + 3)
        return True
    
    def ensure_logged_in(self) -> bool:
        """Ensure we have a valid session, logging in if necessary."""
        if not self.logged_in:
            return self.login()
        return True
    
    # ══════════════════════════════════════════════════════════════════════════
    # Abstract Methods (must be implemented by subclasses)
    # ══════════════════════════════════════════════════════════════════════════
    
    @abstractmethod
    def scrape(self, *args, **kwargs):
        """Main scraping method - must be implemented by subclasses."""
        pass
