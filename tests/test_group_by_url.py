# -*- coding: utf-8 -*-

import random

from qianka.cache.redis_cache import RedisCache

def test_group_by_url():
    cache = RedisCache(hosts=['redis://127.0.0.1',
                              'redis://192.168.1.234'
                              'redis://192.168.1.211'],
                       marshal_module=None)

    random_list = [str(random.randint(1000, 2000)) for i in range(100)]

    url_dict = cache._group_by_url(random_list,
                                   lambda x: cache._get_key(x),
                                   lambda x: cache._key_to_url(cache._get_key(x)))

    for url, pos_key_list in url_dict.items():
        pos_tuple, key_tuple = zip(*pos_key_list)
        for key in key_tuple:
            assert url == cache._key_to_url(key)
