import logging
IP_BROKER = {'host': '10.131.134.95', 'port': 6389, 'db': 14}
IP_COUNT_INFO_KEY = "IP_COUNT"  # Key to hash
IP_POINTER_COUNT_KEY = "ipPointer"
IP_CHANGED_KEY = "ip_changed"
IP_COUNT_KEY = "ip_count"
IP_LIVE_POOL_KEY = "IP_LIVE_POOL_KEY"  # Key to pickle ips
IP_DIED_POOL_KEY = "IP_DIED_POOL_KEY"  # Key to hash
LOG_LEVEL = "INFO"


def get_logger(logger_name):

    logger = logging.getLogger(logger_name)
    logger.setLevel(LOG_LEVEL)
    sh = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] '
                                  '%(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger

import redis
broker = redis.StrictRedis(**IP_BROKER)


def get_current_ip():
    return broker.hget(IP_COUNT_INFO_KEY,
                       IP_COUNT_KEY,) or 0  # Update ip

def get_current_pointer():
    return broker.hget(IP_COUNT_INFO_KEY,
                       IP_POINTER_COUNT_KEY,) or 0  # Update ip
