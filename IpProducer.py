#!/usr/bin/python
import redis
import pickle
import time
import IpBroker
import requests
import threading
import gzip
from cStringIO import StringIO
from Queue import Queue

requests.HTTPErrorDEFAULT_RETRIES = 2

logger = IpBroker.get_logger("IP_PRODUCER")

THREAD_COUNT = 300


class IpProducer(object):
    """IpProducer to put proxy ip into pool, port and type
    """
    def __init__(self):
        self.broker = redis.StrictRedis(**IpBroker.IP_BROKER)

    def putIps(self, ips):
        """Get length ips from pool,return ip dict
            ip -> type&port
            http == port + 100000
            https == port + 1000000
            sock5 == port + 10000000
        """
        p_obj = pickle.dumps(ips)
        fgz = StringIO()
        gzip_obj = gzip.GzipFile(mode='wb', fileobj=fgz)
        gzip_obj.write(p_obj)
        gzip_obj.close()
        data_gzed = fgz.getvalue()
        self.broker.set(IpBroker.IP_LIVE_POOL_KEY, data_gzed)
        self.broker.hset(IpBroker.IP_COUNT_INFO_KEY,
                         IpBroker.IP_CHANGED_KEY, 1)  # Update ip
        logger.info("Set new ips")
        time.sleep(2)
        self.broker.hset(IpBroker.IP_COUNT_INFO_KEY,
                         IpBroker.IP_CHANGED_KEY, 0)  # Update ip
        self.broker.hset(IpBroker.IP_COUNT_INFO_KEY,
                         IpBroker.IP_COUNT_KEY, len(ips))  # Update ip

    def get_block_ip(self):
        """Set block info to ip pool
            ip -> bad_count
        """
        ans = {}
        ips = self.broker.hkeys(IpBroker.IP_DIED_POOL_KEY)
        for ip in ips:
            ans[ip] = self.broker.hget(IpBroker.IP_DIED_POOL_KEY, ip)
        return ans


def update_ips():
    producer = IpProducer()
    url = 'theurl'
    print url
    try:
        r = requests.get(url, timeout=9).text.split("\r\n")
        for l in r:
            s.add(l)
    except:
        pass
    print len(s)
    blocked_ip = producer.get_block_ip()
    all_ips = Queue()
    live_ips = Queue()
    for line in s:
        try:
            ip, port = line.split(":")
            all_ips.put((ip, port))
        except:
            pass
    thread_pool = []
    logger.info("All ips %s " % all_ips.qsize())
    for i in xrange(0, THREAD_COUNT):
        t = threading.Thread(target=test_ip,
                             args=(all_ips, live_ips, blocked_ip))
        thread_pool.append(t)
    for t in thread_pool:
        t.start()
    for t in thread_pool:
        t.join()
    logger.info("The live ip len is %s" % live_ips.qsize())
    put_ips = {}
    while True:
        try:
            (ip, port) = live_ips.get(block=False)
            put_ips[ip] = port
        except:
            break
        # logger.info("%s %s " % (ip, port))
    producer.putIps(put_ips)


def test_ip(all_ips, live_ips, blocked_ip):
    url = "http://baidu.com"
    headers = {}
    # headers['User-Agent'] = 'curl/7.37.1'
    while True:
        try:
            l = all_ips.qsize()
            if l == 0:
                raise Exception
            else:
                pass
            (ip, port) = all_ips.get(block=False)
            if ip in blocked_ip:
                continue
        except:
            # logger.info("I am out")
            break
        pxy = {}
        pxy['http'] = 'http://%s:%s' % (ip, port)
        # logger.info("Doing %s" % pxy)
        try:
            # requests.get(url, proxies=pxy, allow_redirects=False,
            #              headers=headers, timeout=10).text
            # logger.info(res)
            new_port = int(port) + 100000  # http + 100000
            live_ips.put((ip, new_port))
            # logger.info("Put %s %s Live len:%s All Len: %s"
            #            % (ip, port, live_ips.qsize(), all_ips.qsize()))
        except:
            continue


def main():
    while True:
        try:
            update_ips()
            logger.info("Sleep 5 * 60 second")
            time.sleep(1 * 60)
        except Exception as e:
            time.sleep(15)
            logger.error(e)


if __name__ == "__main__":
    main()
