from __future__ import absolute_import

from task_queue import  celeryconfig
from celery import Celery

app = Celery('tasks', broker=celeryconfig.BROKER_URL)

@app.task
def test():
    return 'hello'