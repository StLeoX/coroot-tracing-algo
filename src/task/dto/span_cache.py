from cachetools import TTLCache


# https://cachetools.readthedocs.io/en/v5.5.0/#cachetools.TTLCache
# https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_Recently_Used_(LRU)
class SpanCache(TTLCache):
    '''
    键值对 (span_id, span)。
    '''

    def __init__(self, maxsize, ttl, timer):
        super().__init__(maxsize, ttl, timer)

    def popitem(self):
        span_id, span = super().popitem()
        print('Key "%s" overflowed with value "%s"' % (span_id, span.container_id))
        return span_id, span

    def expire(self, time=None):
        items = super().expire(time)
        if items is None:
            return None
        for span_id, span in items:
            print('Key "%s" expired with value "%s"' % (span_id, span.container_id))
        return items
