# -*- coding: utf-8 -*-

from qianka.cache import QKCache

def test_set_get_many():
    cache = QKCache()
    cache.configure({
        'CACHE_NODES': {'default': ('redis', ['redis://127.0.0.1', 'redis://192.168.1.234'])},
        'CACHE_ENABLED': True,
        'CACHE_KEY_PREFIX': '',
    })

    assert cache.set_many({'a': 123, 'b': 21, 'c': 123}, timeout=3) == True

    a, b, c = cache.get_many('a', 'b', 'c')
    assert a == 123 and b == 21 and c == 123

    from time import sleep
    sleep(5)

    a, b, c = cache.get_many('a', 'b', 'c')
    assert a is None and b is None and c is None
