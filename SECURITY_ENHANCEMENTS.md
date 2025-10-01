# Security Enhancements - Anti-Scraping Protection

## Problem
- Case search requests increased from 300 to 4300 today
- Someone likely bypassed or automated the reCAPTCHA protection
- Need additional layers of protection against scraping

## Implemented Solutions

### 1. Rate Limiting System
**File:** `src/dol_analytics/middleware/rate_limiter.py`

- **IP-based rate limiting** with different limits per endpoint
- **Sliding window approach** for accurate rate limiting
- **Suspicious activity tracking** for IPs that consistently hit limits

**Rate Limits:**
- Company search: 10 requests/minute
- Company cases: 5 requests/minute  
- All other endpoints: No rate limiting

### 2. Enhanced Logging & Monitoring
**Added to company search endpoints only:**

- **IP address logging** for company search requests
- **Query parameter logging** (truncated for privacy)
- **reCAPTCHA failure logging** with IP addresses
- **Request pattern tracking**

**Example logs:**
```
🔍 Company search request from IP: 192.168.1.100, query: 'Google Inc...'
❌ Invalid reCAPTCHA from IP: 192.168.1.100
```

### 3. reCAPTCHA Protection
**Protected endpoints:**
- `POST /api/data/company-search` - reCAPTCHA + Rate limiting (10 req/min)
- `POST /api/data/company-cases` - reCAPTCHA + Rate limiting (5 req/min)
- `POST /api/data/updated-cases` - No protection (open access)
- All other endpoints - No protection (open access)

### 4. Admin Monitoring Endpoints
**New endpoints for monitoring:**

- `GET /api/data/admin/rate-limit-stats` - View suspicious IPs and rate limit statistics
- `POST /api/data/admin/block-ip` - Manually block abusive IP addresses

### 5. Automatic IP Blocking
- IPs that hit rate limits repeatedly are flagged as suspicious
- Automatic temporary blocking for aggressive scrapers
- Manual blocking capability for confirmed bad actors

## Usage

### Monitor Suspicious Activity
```bash
curl http://localhost:8000/api/data/admin/rate-limit-stats
```

### Block an IP Address
```bash
curl -X POST "http://localhost:8000/api/data/admin/block-ip?ip_address=192.168.1.100&duration=7200"
```

## Rate Limit Response
When rate limited, clients receive:
```json
{
  "detail": "Too many requests. Limit: 10 per 60 seconds",
  "retry_after": 45
}
```
HTTP Status: `429 Too Many Requests`

## Protection Layers

1. **reCAPTCHA verification** - Prevents basic automation
2. **Rate limiting** - Prevents high-volume requests
3. **IP tracking** - Identifies suspicious patterns  
4. **Logging** - Provides audit trail
5. **Manual blocking** - Emergency response capability

## Next Steps

1. **Monitor the logs** for patterns in the scraping attempts
2. **Check rate limit stats** regularly to identify new threats
3. **Consider additional measures** if scraping continues:
   - User-Agent filtering
   - Geographic IP blocking
   - Request signature validation
   - Honeypot endpoints

## Files Modified

- `src/dol_analytics/middleware/rate_limiter.py` (NEW)
- `src/dol_analytics/middleware/__init__.py` (NEW)
- `src/dol_analytics/api/routes/data.py` (MODIFIED)
- `src/dol_analytics/models/schemas.py` (MODIFIED)

## Testing

The rate limiting is now active. Test with:
```bash
# This should work
curl -X POST "http://localhost:8000/api/data/company-search" -H "Content-Type: application/json" -d '{"query": "Google", "recaptcha_token": "valid_token"}'

# After 10 requests in a minute, you should get 429 Too Many Requests
```

**The API is now significantly more protected against scraping attempts.**
