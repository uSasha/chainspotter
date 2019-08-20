import time
from typing import List

import redis


class ClickChain:
    def __init__(self, prefix: str, redis_connection: redis.client.Redis, limit: int = None) -> None:
        """
        Should be used to store user items in set format
        :param prefix: each entity should have unique prefix
        :param redis_connection: redis connection pool
        """
        self.redis = redis_connection
        self.prefix = prefix
        self.limit = limit

    def last_n_pcs(self, user_id: int, count: int = None) -> List[int]:
        stream_content = self.redis.xrevrange(f'{self.prefix}_{user_id}', count=count)
        return [int(log[b'item']) for timestamp, log in stream_content][::-1]

    def last_n_hours(self, user_id: int, hours: int = 24) -> List[int]:
        now_ms = int(time.time() * 1000)
        stream_content = self.redis.xrange(f'{self.prefix}_{user_id}',
                                           min=now_ms - hours * 60 * 60 * 1000)
        return [int(log[b'item']) for timestamp, log in stream_content]

    def set(self, user_id: int, item: int) -> None:
        self.redis.xadd(f'{self.prefix}_{user_id}', {'item': item}, maxlen=self.limit)

    def __iter__(self) -> str:
        cursor = 0
        streams = [None]
        while streams:
            cursor, streams = self.redis.scan(cursor=cursor, match=self.prefix + '*', count=1000)
            for stream in streams:
                yield int(stream.decode().replace(self.prefix + '_', ''))


def with_cache(cache):
    def meta_wrapper(f):
        def wrapper(*args, **kwargs):
            if 'user_id' in kwargs and 'item' in kwargs:
                cache.set(user_id=kwargs['user_id'], item=kwargs['item'])
            return f(*args, **kwargs)

        return wrapper

    return meta_wrapper
