import inspect
import logging
import time
from typing import List

import redis

logger = logging.getLogger('drainpipe')


class ClickChain:
    """
    Redis Streams wrapper to track, store and query by count or time user-item interactions history
    """
    def __init__(self, prefix: str, redis_connection: redis.client.Redis, limit: int = None) -> None:
        """
        :param prefix: stream name = prefix + '_' + user_id
        :param redis_connection: redis connection pool
        :param limit: user history length
        """
        self.redis = redis_connection
        self.prefix = prefix
        self.limit = limit
        logging.info('New clickchain created, prefix: %s, limit: %s', prefix, limit)

    def last_n_pcs(self, user_id: int, count: int = None) -> List[int]:
        """
        Get last N items used by user
        :param user_id: user ID
        :param count: number of items
        :return: list if item IDs from old to new ones
        """
        stream_content = self.redis.xrevrange(f'{self.prefix}_{user_id}', count=count)
        return [int(log[b'item']) for timestamp, log in stream_content][::-1]

    def last_n_hours(self, user_id: int, hours: int = 24) -> List[int]:
        """
        Get items used by user in last N hours
        :param user_id: user ID
        :param hours: number of hours
        :return: list if item IDs from old to new ones
        """
        now_ms = int(time.time() * 1000)
        stream_content = self.redis.xrange(f'{self.prefix}_{user_id}', min=now_ms - hours * 60 * 60 * 1000)
        return [int(log[b'item']) for timestamp, log in stream_content]

    def add(self, user_id: int, item: int) -> None:
        """
        Add item to user history
        :param user_id: user ID
        :param item: item ID
        """
        self.redis.xadd(f'{self.prefix}_{user_id}', {'item': item}, maxlen=self.limit)
        logging.debug('Added new item %s for user %s clickchain', item, user_id)

    def __iter__(self) -> str:
        """
        Iterate over user IDs stored in Redis
        :return:
        """
        cursor = 0
        streams = [None]
        while streams:
            cursor, streams = self.redis.scan(cursor=cursor, match=self.prefix + '*', count=1000)
            for stream in streams:
                yield int(stream.decode().replace(self.prefix + '_', ''))


def to_chain(prefix: str, redis_connection: redis.client.Redis, limit: int = None,
             user_id_arg: str = 'user_id', item_id_arg: str = 'item_id'):
    """
    Decorator to store user-item interactions to Redis Streams
    :param prefix: stream name = prefix + '_' + user_id
    :param redis_connection: redis connection pool
    :param limit: user history length
    :param user_id_arg: user_id argument name in wrapped function
    :param item_id_arg: item_id argument name in wrapped function
    """
    def dump_args(func):
        def wrapper(*args, **kwargs):
            func_args = inspect.signature(func).bind(*args, **kwargs).arguments
            user = func_args[user_id_arg]
            item = func_args[item_id_arg]
            redis_connection.xadd(f'{prefix}_{user}', {'item': item}, maxlen=limit)
            logging.debug('Added new item %s for user %s clickchain', item, user)
            return func(*args, **kwargs)
        return wrapper
    return dump_args
