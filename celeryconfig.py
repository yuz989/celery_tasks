# -*- coding: utf-8 -*-

import os

class CeleryConfig:
    BROKER_URL              = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    CELERY_RESULT_BACKEND   = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    SQLALCHEMY_DATABASE_URI = os.environ['SQLALCHEMY_DATABASE_URI']
    REDIS_HOST              = os.environ['REDIS_HOST']
    ELASTICSEARCH_HOST      = os.environ['ELASTICSEARCH_HOST']
    GOOGLE_SERVICE_ACCOUNT_EMAIL   = '366013181280-ki7qllkqg27bqtatd5tnrsghmd7g8ass@developer.gserviceaccount.com'
    GOOGLE_SERVICE_CREDENTIAL_PATH = '/QLand_privatekey.pem'
