import os
from celery import Celery

class Celery_Config:

    BROKER_URL            = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    CELERY_RESULT_BACKEND = 'redis://%s:6379/0' % os.environ['REDIS_HOST']


app = Celery('tasks')
app.config_from_object('Celery_Config')

@app.task(name='task_queue.celeryapp.test')
def test():
    return 'hello celery'