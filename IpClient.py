#!/usr/bin/env python
import redis
import IpBroker
import pickle
import time
import gzip
from cStringIO import StringIO

logger = IpBroker.get_logger("IP_CLIENT")


class IpClient(object):
    """IpClient for get proxy ip, port and type
    """
    def __init__(self):
        self.broker = redis.StrictRedis(**IpBroker.IP_BROKER)
        self.pointer = 0
        self.ip_cache = {}
        self.ip_len = 0
        self.ips = []
        self.expire_time = 1 * 60 * 60  # One hour

    def getIps(self, length=1):
        """Get length ips from pool,return ip dict
            ip -> type&port
            http == port + 100000
            https == port + 1000000
            sock5 == port + 10000000
        """
        self.pointer = self.broker.hincrby(IpBroker.IP_COUNT_INFO_KEY, IpBroker.IP_POINTER_COUNT_KEY, length) - length  # Get the pointer
        changed = self.broker.hget(IpBroker.IP_COUNT_INFO_KEY, IpBroker.IP_CHANGED_KEY) or 0  # Update ip
        if int(changed) == 1 or len(self.ip_cache) == 0:
            while True:
                logger.debug("Get new ips %s" % changed)
                pk = self.broker.get(IpBroker.IP_LIVE_POOL_KEY)
                if pk is None:
                    logger.debug("No Ip, sleep 1 second")
                    time.sleep(1)
                    continue
                else:
                    buf = StringIO(pk)
                    gzip_obj = gzip.GzipFile(mode='rb', fileobj=buf)
                    p_obj = gzip_obj.read()
                    gzip_obj.close()
                    self.ip_cache = pickle.loads(p_obj)
                    self.ips = self.ip_cache.keys()
                    self.ip_len = len(self.ips)
                    logger.debug("Ip len %s" % self.ip_len)
                    break
        ans = {}
        for i in xrange(0, length):
            try:
                ip = self.ips[(i + self.pointer) % self.ip_len]
                port = self.ip_cache[ip]
            except:
                continue
            ans[ip] = port
        return ans

    def setIps(self, bad_ip_info):
        """Set block info to ip pool
            ip -> bad_count
        """
        for ip in bad_ip_info:
            try:
                self.broker.hincrby(IpBroker.IP_DIED_POOL_KEY, ip, bad_ip_info[ip])
                self.broker.expire(IpBroker.IP_DIED_POOL_KEY, self.expire_time)
            except Exception as e:
                logger.error(e)

if __name__ == "__main__":
    logger.info("Test ip client")
    ipc = IpClient()
    print ipc.getIps(10)
    print ipc.getIps(12)
