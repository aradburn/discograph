"""
This module defines utility functions and decorators for the Discograph system.

It primarily includes a `limit` decorator for implementing rate limiting on
Flask application endpoints.

Key functionalities include:
    - **`limit`**: A decorator that enforces rate limiting on a Flask
      view function. It uses a Redis client (or a fake Redis client for
      testing) to track the number of requests made by a specific client
      within a defined time period.

The `limit` decorator interacts with the following components:
    - `flask.request`: For accessing information about the current request,
      such as the endpoint and remote address.
    - `flask.g`: For storing view limits information that can be accessed
      later in the request lifecycle.
    - `fakeredis.FakeStrictRedis`: A fake Redis client used for testing,
      providing a in-memory implementation of the Redis API.
    - `redis.StrictRedis`: the standard redis client, not used in this version.
    - `exceptions.RateLimitError`: A custom exception raised when the rate
      limit is exceeded.
    - `logging`: For logging operations.

The module utilizes `logging` for logging operations, `functools` for the
`wraps` decorator, `time` for time related operations, `fakeredis` for the fake
redis client and `flask` to handle http request. It uses `discograph` library
for the `RateLimitError` exception.
"""

import functools
import logging
import time

import fakeredis
import flask

from discograph import exceptions

log = logging.getLogger(__name__)
"""
The logger for the decorators module.
"""

redis_client = fakeredis.FakeStrictRedis()
# redis_client = redis.StrictRedis()
"""
The redis client, `fakeredis.FakeStrictRedis` is used for testing.
"""


def limit(max_requests=10, period=60):
    """
    A decorator that enforces rate limiting on a Flask view function.

    This decorator limits the number of requests a client can make to a
    specific endpoint within a defined time period. It uses a Redis client
    to track the number of requests and raises a `RateLimitError` if the limit
    is exceeded.

    Args:
        max_requests (int, optional): The maximum number of requests allowed
            within the period. Defaults to 10.
        period (int, optional): The time period (in seconds) within which the
            `max_requests` apply. Defaults to 60.

    Returns:
        Callable: The decorated view function.

    Raises:
        exceptions.RateLimitError: If the rate limit is exceeded.
    """

    def decorator(f):
        """
        The actual decorator that wraps the view function.

        Args:
            f (Callable): The view function to decorate.

        Returns:
            Callable: The wrapped view function.
        """

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            """
            The wrapped view function that performs the rate limiting check.

            Args:
                *args: Variable length argument list.
                **kwargs: Arbitrary keyword arguments.

            Returns:
                Any: The return value of the decorated view function.

            Raises:
                exceptions.RateLimitError: If the rate limit is exceeded.
            """
            # For testing error handlers:
            # max_requests = 2

            key = f"ratelimit:{flask.request.endpoint}:{flask.request.remote_addr}"
            """
            The Redis key used to track requests for this endpoint and client.
            """

            try:
                remaining = max_requests - int(redis_client.get(key))
                """
                Calculate the remaining requests by subtracting the current
                request count from the maximum allowed.
                """
            except (ValueError, TypeError):
                """
                If the Redis value is not an integer or is None.
                """
                remaining = max_requests
                """
                Reset the remaining count to the maximum.
                """
                redis_client.setex(key, period, 0)
                """
                Initialize the Redis key with a request count of 0
                and an expiration time.
                """

            ttl = redis_client.ttl(key)
            """Get the time to live (TTL) of the Redis key."""
            if not ttl:
                """If the key is expired."""
                redis_client.expire(key, period)
                """Set the expiration time for the key."""
                ttl = period
                """Update the ttl."""

            flask.g.view_limits = (max_requests, remaining - 1, time.time() + ttl)
            """
            Store view limits information in the Flask global context for later use.
            """

            if 0 < remaining:
                """If there are remaining requests."""
                redis_client.incr(key, 1)
                """Increment the request count."""
                log.debug(f"key: {key}, remaining: {remaining}, ttl: {ttl}")
                """Execute the decorated view function."""
                return f(*args, **kwargs)
            else:
                """If the rate limit is exceeded."""
                log.debug(f"key: {key}, remaining: {remaining}, ttl: {ttl}")
                """Raise a RateLimitError."""
                raise exceptions.RateLimitError()

        return wrapped

    return decorator
