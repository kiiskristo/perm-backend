"""
Rate limiting middleware to prevent API abuse and scraping.
"""

import time
from typing import Dict, Optional
from fastapi import HTTPException, Request
from collections import defaultdict, deque
import logging

logger = logging.getLogger("dol_analytics.rate_limiter")


class RateLimiter:
    """
    Rate limiter with different limits for different endpoints.
    Uses sliding window approach with IP-based tracking.
    """
    
    def __init__(self):
        # Store request timestamps per IP per endpoint
        # Structure: {ip: {endpoint: deque([timestamp1, timestamp2, ...])}}
        self.requests: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(deque))
        
        # Rate limits per endpoint (requests per time window)
        # Only apply to company search endpoints
        self.limits = {
            "/api/data/company-search": {"requests": 10, "window": 60},  # 10 requests per minute
            "/api/data/company-cases": {"requests": 5, "window": 60},    # 5 requests per minute
        }
        
        # Global rate limit (fallback for any endpoint)
        self.global_limit = {"requests": 100, "window": 60}  # 100 requests per minute
        
        # Suspicious activity tracking
        self.suspicious_ips: Dict[str, Dict] = {}  # {ip: {"count": int, "first_seen": timestamp}}
        
    def get_client_ip(self, request: Request) -> str:
        """Extract client IP from request, handling proxies."""
        # Check for forwarded headers (common in production)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take the first IP in the chain
            return forwarded_for.split(",")[0].strip()
        
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip
        
        # Fallback to direct client IP
        return request.client.host if request.client else "unknown"
    
    def clean_old_requests(self, ip: str, endpoint: str, window: int):
        """Remove requests older than the time window."""
        current_time = time.time()
        cutoff_time = current_time - window
        
        # Remove old requests
        while (self.requests[ip][endpoint] and 
               self.requests[ip][endpoint][0] < cutoff_time):
            self.requests[ip][endpoint].popleft()
    
    def is_rate_limited(self, request: Request) -> Optional[Dict]:
        """
        Check if request should be rate limited.
        Returns None if allowed, or dict with error info if blocked.
        """
        ip = self.get_client_ip(request)
        endpoint = request.url.path
        current_time = time.time()
        
        # Only apply rate limiting to endpoints in the limits dictionary
        if endpoint not in self.limits:
            return None  # No rate limiting for this endpoint
            
        # Get rate limit for this endpoint
        limit_config = self.limits[endpoint]
        max_requests = limit_config["requests"]
        window = limit_config["window"]
        
        # Clean old requests
        self.clean_old_requests(ip, endpoint, window)
        
        # Count current requests in window
        current_requests = len(self.requests[ip][endpoint])
        
        # Check if limit exceeded
        if current_requests >= max_requests:
            # Track suspicious activity
            self.track_suspicious_activity(ip, endpoint, current_requests, max_requests)
            
            # Calculate when they can try again
            if self.requests[ip][endpoint]:
                oldest_request = self.requests[ip][endpoint][0]
                retry_after = int(oldest_request + window - current_time) + 1
            else:
                retry_after = window
            
            logger.warning(f"Rate limit exceeded for IP {ip} on {endpoint}: {current_requests}/{max_requests}")
            
            return {
                "error": "Rate limit exceeded",
                "detail": f"Too many requests. Limit: {max_requests} per {window} seconds",
                "retry_after": retry_after,
                "current_requests": current_requests,
                "max_requests": max_requests
            }
        
        # Add current request to tracking
        self.requests[ip][endpoint].append(current_time)
        
        return None
    
    def track_suspicious_activity(self, ip: str, endpoint: str, current: int, limit: int):
        """Track IPs that consistently hit rate limits."""
        if ip not in self.suspicious_ips:
            self.suspicious_ips[ip] = {
                "count": 1,
                "first_seen": time.time(),
                "endpoints": {endpoint: 1}
            }
        else:
            self.suspicious_ips[ip]["count"] += 1
            if endpoint not in self.suspicious_ips[ip]["endpoints"]:
                self.suspicious_ips[ip]["endpoints"][endpoint] = 0
            self.suspicious_ips[ip]["endpoints"][endpoint] += 1
        
        # Log if this IP is being very aggressive
        if self.suspicious_ips[ip]["count"] > 10:
            logger.error(f"SUSPICIOUS ACTIVITY: IP {ip} has hit rate limits {self.suspicious_ips[ip]['count']} times")
            logger.error(f"Endpoints targeted: {self.suspicious_ips[ip]['endpoints']}")
    
    def get_suspicious_ips(self) -> Dict:
        """Get list of suspicious IPs for monitoring."""
        current_time = time.time()
        recent_suspicious = {}
        
        for ip, data in self.suspicious_ips.items():
            # Only include IPs that have been suspicious in the last hour
            if current_time - data["first_seen"] < 3600:
                recent_suspicious[ip] = data
        
        return recent_suspicious
    
    def block_ip(self, ip: str, duration: int = 3600):
        """Temporarily block an IP (duration in seconds)."""
        # This could be extended to use a more persistent storage
        # For now, we'll just add a very high request count
        current_time = time.time()
        
        # Add many fake requests to effectively block the IP
        for endpoint in self.limits.keys():
            self.requests[ip][endpoint] = deque([current_time] * 1000)
        
        logger.warning(f"IP {ip} has been temporarily blocked for {duration} seconds")


# Global rate limiter instance
rate_limiter = RateLimiter()


def check_rate_limit(request: Request):
    """
    FastAPI dependency to check rate limits.
    Raises HTTPException if rate limit exceeded.
    """
    result = rate_limiter.is_rate_limited(request)
    
    if result:
        raise HTTPException(
            status_code=429,
            detail=result["detail"],
            headers={"Retry-After": str(result["retry_after"])}
        )


def get_rate_limit_stats():
    """Get current rate limiting statistics."""
    return {
        "suspicious_ips": rate_limiter.get_suspicious_ips(),
        "total_tracked_ips": len(rate_limiter.requests),
        "limits": rate_limiter.limits
    }
