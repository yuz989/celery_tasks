import os
from celery import Celery

class Celery_Config:

    BROKER_URL = 'redis://%s:6379/0' % os.environ['REDIS_HOST']


app = Celery('tasks', broker=Celery_Config.BROKER_URL)

@app.task(name='task_queue.celeryapp.test')
def test():
    return 'hello'