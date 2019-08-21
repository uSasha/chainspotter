# chainspotter
Redis Streams wrapper to track, store and query user-item interactions history by item count or time.

#### Usage example
Producer:
```python
import redis
import chainspotter


@chainspotter.to_chain('user_history', redis.Redis(), limit=20)
def show_item_to_user(user_id, item_id):
    print('item_id')

for i in range(5):
    show_item_to_user(2, i)
```

Consumer:
```python
import redis
import chainspotter

chain = chainspotter.ClickChain('user_history', redis.Redis())
chain.last_n_pcs(2)

> [0, 1, 2, 3, 4]
```
