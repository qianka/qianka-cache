# -*- coding: utf-8 -*-
import redis

from .hash_ring import HashRing


__all__ = ['RedisCache']


#
# cache api see
# http://werkzeug.pocoo.org/docs/0.11/contrib/cache/
#
class RedisCache(object):
    """RedisCache layer with consistent hashing multiple instances support
    """

    def __init__(self, hosts, marshal_module,
                 key_prefix='', default_timeout=300):
        """port, db, password, weight are optional

        marshal_module should contain standard python marshal method

          :loads:
          :dumps:

        hosts examples:

           [
               'redis://localhost',
               'redis://localhost:6380/',
               'redis://localhost:6381/1',
               'redis://:pass@localhost:6382/2',
           ]

        or with weights (all hosts' weights must be provided together!)

           [
               ('redis://localhost', 2),
               ('redis://localhost:6380/', 5),
               # ...
           ]
        """
        self.key_prefix = key_prefix
        self.default_timeout = default_timeout
        self.marshal_module = marshal_module

        if type(hosts[0]) == tuple and len(hosts[0]) > 1:
            nodes = [x for x in map(lambda _: _[0], hosts)]
            weights = {x: y for x, y in hosts}
        else:
            nodes = hosts
            weights = None

        self.ring = HashRing(nodes, weights)
        self.clients = {}

        self._nodes = nodes


    def reset(self):
        """close all connection, remove all client instances
        """
        for k in list(self.clients.keys()):
            r = self.clients.get(k)
            # r.connection_pool.disconnect()
            del r


    def _get_expiration(self, timeout):
        if timeout is None:
            timeout = self.default_timeout
        if timeout == 0:
            timeout = -1
        return timeout


    def _get_key(self, k):
        return self.key_prefix + k


    def _get_connection(self, url):
        if url not in self.clients:
            r = redis.StrictRedis.from_url(url)
            self.clients[url] = r
        return self.clients.get(url)


    def _key_to_conn(self, key):
        url = self.ring.get_node(key)
        return self._get_connection(url)


    def dump_object(self, value):
        """Dumps an object into a string for redis.  By default it serializes
        integers as regular string and pickle dumps everything else.
        """
        if type(value) == int:
            value = str(value).encode('ascii')
        return self.marshal_module.dumps(value)


    def load_object(self, value):
        """The reversal of :meth:`dump_object`.  This might be called with
        None.
        """
        if value is None:
            return None

        value = self.marshal_module.loads(value)
        try:
            return int(value)
        except (ValueError, TypeError):
            return value


    def get(self, key, raw=False):
        k = self._get_key(key)
        client = self._key_to_conn(k)

        if raw:
            return client.get(k)

        return self.load_object(client.get(k))


    def set(self, key, value, timeout=None):
        timeout = self._get_expiration(timeout)
        dump = self.dump_object(value)

        k = self._get_key(key)
        client = self._key_to_conn(k)

        if timeout == -1:
            result = client.set(name=k,
                                      value=dump)
        else:
            result = client.setex(name=k, time=timeout, value=dump)
        return result


    def add(self, key, value, timeout=None):
        timeout = self._get_expiration(timeout)
        dump = self.dump_object(value)

        k = self._get_key(key)
        client = self._key_to_conn(k)

        return (
            client.setnx(name=k, value=dump) and
            client.expire(name=k, time=timeout)
        )


    def get_many(self, *keys):
        # TODO: maybe indexing keys first, then group by
        # connections, then restore the result sort
        return [x for x in map(self.get, keys)]


    def set_many(self, mapping, timeout=None):
        # TODO: maybe indexing keys first, then group by
        # connections, then restore the result sort
        return [x for x in
                map(lambda _: self.set(_[0], _[1], timeout),
                    list(mapping.items()))]


    def delete(self, key):
        k = self._get_key(key)
        client = self._key_to_conn(k)

        return client.delete(k)


    def delete_many(self, *keys):
        if not keys:
            return
        return [x for x in map(self.delete, keys)]


    def clear(self):
        status = False

        for url in self._nodes:
            client = self._get_connection(url)

            k = self._get_key('*')

            if self.key_prefix:
                keys = client.keys(k)
                if keys:
                    status = client.delete(*keys)
            else:
                status = client.flushdb()
        return status

    def inc(self, key, delta=1):
        k = self._get_key(key)
        client = self._key_to_conn(k)
        return client.incr(name=k, amount=delta)


    def dec(self, key, delta=1):
        k = self._get_key(key)
        client = self._key_to_conn(k)
        return client.decr(name=k, amount=delta)
