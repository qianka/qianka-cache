# -*- coding: utf-8 -*-
import logging
from threading import Lock

import msgpack
from werkzeug.contrib.cache import NullCache

from .redis_cache import RedisCache
from .utils._collections import ScopedRegistry, ThreadLocalRegistry


logger = logging.getLogger('qianka.cache')


class QKCache(object):
    """
    Usage:
    ```
    from application import cache
    cache.configure()
    value = cache.get('name')
    cache('namespace').set('key', value)
    ```
    """

    def __init__(self):
        self._config = {}
        self._config.setdefault('CACHE_ENABLED', False)
        self._config.setdefault('CACHE_KEY_PREFIX', 'qianka')
        self._config.setdefault('CACHE_DEFAULT_TIMEOUT', 300)
        self._config.setdefault('CACHE_NODES',
                                {'default': ['redis://127.0.0.1']})

        self._instances = {}
        self._instances_lock = Lock()

    def get_instances(self):
        return self._instances

    @property
    def config(self):
        return self._config

    def configure(self, config=None, **kwargs):
        """
        - CACHE_ENABLED
        - CACHE_NODES
        - CACHE_KEY_PREFIX
        - CACHE_DEFAULT_TIMEOUT     in seconds

        !!default is a must!! NODES could be:

        {
          'default': ['redis://127.0.0.1', 'redis://127.0.0.1:6380'],

          # with weight
          'another': [('redis://192.168.1.100/1', 2), ('redis://192.168.1.200/0', 5)],
        }
        """
        if config:
            self._config.update(config)
        if kwargs:
            self._config.update(kwargs)

    def get_cache(self, name='default'):
        """return a RedisCache instance, initial it for the first time
        """
        with self._instances_lock:

            instances = self.get_instances()

            if name in instances:
                return instances[name]

            cache_enabled = self._config['CACHE_ENABLED']
            if not cache_enabled:
                logger.info('disabled, init null cache')
                instances[name] = NullCache()
                return instances[name]

            cfg = self._config['CACHE_NODES'][name]
            if type(cfg) in (list, tuple):
                backend = cfg[0]
                nodes = cfg[1]
            else:
                backend = cfg
                nodes = []

            QKCache._check_backend(backend)

            key_prefix = self._config['CACHE_KEY_PREFIX']
            timeout = self._config['CACHE_DEFAULT_TIMEOUT']

            if backend == 'redis':
                logger.info('init redis cache with nodes: %s' % nodes)
                inst = RedisCache(nodes,
                                  key_prefix=key_prefix,
                                  default_timeout=timeout,
                                  marshal_module=msgpack)
            elif backend == 'memcached':
                logger.info('init redis cache with nodes: %s' % nodes)
                raise NotImplementedError('unsupported cache backend: %s' % \
                                          backend)
            else:
                logger.info('init null cache')
                inst = NullCache()

            instances[name] = inst
            return inst

    def reset(self):
        """
        close all instances
        """
        with self._instances_lock:
            instances = self.get_instances()
            for instance in instances.values():
                instance.reset()
            instances.clear()

    @staticmethod
    def _check_backend(backend):
        """preform various checking
        """
        if backend.lower() not in \
           ('redis', 'null'):
            raise NotImplementedError('unsupported cache backend: %s' % backend)

    #
    # sugar
    #

    def __call__(self, name='default'):
        return self.get_cache(name)

    def __getattr__(self, name):
        """
        magic ruby ``method_missing'' equivalent
        """
        if name not in (
            'add',
            'set',
            'get',
            'delete',
            'set_many',
            'get_many',
            'delete_many',
            'inc',
            'dec',
            'clear',
        ):
            raise RuntimeError('unknown cache action: %s'\
                               'please consult the cache API' % name)

        # delegate to default cache instance
        return getattr(self.get_cache(), name)


def scoped_cache(cache, scopefunc=None):
    if scopefunc:
        cache.registry = ScopedRegistry(lambda: {}, scopefunc)
    else:
        cache.registry = ThreadLocalRegistry(lambda: {})

    cache.get_instances = lambda: cache.registry()
