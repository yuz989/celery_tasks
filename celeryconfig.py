# coding: utf-8

import os

class CeleryConfig:
    BROKER_URL              = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    CELERY_RESULT_BACKEND   = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    SQLALCHEMY_DATABASE_URI = os.environ['SQLALCHEMY_DATABASE_URI']
    AWS_ACCESS_KEY          = os.environ['AWS_ACCESS_KEY']
    AWS_SECRET_KEY          = os.environ['AWS_SECRET_KEY']
    AWS_S3_BUCKET           = 'qllco'

    REDIS_HOST              = os.environ['REDIS_HOST']
    ELASTICSEARCH_HOST      = os.environ['ELASTICSEARCH_HOST']
    GOOGLE_SERVICE_ACCOUNT_EMAIL   = '366013181280-ki7qllkqg27bqtatd5tnrsghmd7g8ass@developer.gserviceaccount.com'
    GOOGLE_SERVICE_CREDENTIAL_PATH = '/QLand_privatekey.pem'
