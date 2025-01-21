from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request, Response
import os

# Configure rate limits from environment variables (requests per minute)
RATE_LIMIT_DEFAULT = os.getenv('RATE_LIMIT_DEFAULT', '60/minute')  # Default: 60 requests per minute
RATE_LIMIT_DATA = os.getenv('RATE_LIMIT_DATA', '120/minute')      # Data endpoints: 120 requests per minute

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """
    Handler for rate limit exceeded errors.
    Returns a 429 Too Many Requests response with details about the rate limit.
    """
    response = Response(
        content={"detail": f"Rate limit exceeded. {str(exc)}"}, 
        status_code=429
    )
    response.headers["Retry-After"] = str(exc.retry_after)
    return response 