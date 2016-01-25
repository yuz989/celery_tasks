import os

class CeleryConfig:
    BROKER_URL              = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    CELERY_RESULT_BACKEND   = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    SQLALCHEMY_DATABASE_URI = os.environ['SQLALCHEMY_DATABASE_URI']
    REDIS_HOST              = os.environ['REDIS_HOST']
    ELASTICSEARCH_HOST      = os.environ['ELASTICSEARCH_HOST']
