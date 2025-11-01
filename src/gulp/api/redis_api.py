from urllib.parse import urlparse

from muty.log import MutyLogger
from redis.asyncio.client import Redis as AsyncRedis

from gulp.config import GulpConfig


class GulpRedis:
    _instance: "GulpRedis" = None

    def __init__(self):
        self._redis: AsyncRedis = None

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpRedis":
        """
        returns the singleton instance of the Redis client.

        Returns:
            GulpRedis: The singleton instance of the Redis client.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    async def close(self) -> None:
        """
        closes the redis connection.
        """
        if self._redis:
            await self._redis.close()
            MutyLogger.get_instance().info(
                "Redis client %s connection closed", self._redis
            )

    def client(self) -> AsyncRedis:
        """
        gets the redis client, to be called once per process.
        calling it repeatedly in the same process always return the same instance.

        Returns:
            AsyncRedis: The underlying Redis client.
        """
        if not self._redis:
            # first instantiation
            url: str = GulpConfig.get_instance().redis_url()
            self._redis = AsyncRedis.from_url(url)
            url_parts = urlparse(url)
            MutyLogger.get_instance().info(
                "client %s connected to Redis at %s:%d",
                self._redis,
                url_parts.hostname,
                url_parts.port,
            )
        return self._redis
