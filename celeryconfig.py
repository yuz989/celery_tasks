import os

BROKER_URL = 'redis://%s:6379/0' % os.environ['REDIS_HOST']