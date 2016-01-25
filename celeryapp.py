# -.- coding: utf-8 -.-
#TODO reorganize project structure

import os
from celery import Celery
from celeryconfig import CeleryConfig

app = Celery('tasks')
app.config_from_object(CeleryConfig)


### EMail ###
import smtplib
from jinja2 import Environment, PackageLoader
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import utils

class EmailConfig:
    AWS_SES_SMTP_USER     = os.environ['AWS_SES_SMTP_USER']
    AWS_SES_SMTP_PASSWORD = os.environ['AWS_SES_SMTP_PASSWORD']
    AWS_SES_SMTP_HOST     = os.environ['AWS_SES_SMTP_HOST']
    AWS_SES_SMTP_PORTS    = [25, 465, 587]

def _smtp_sendMail(receiver, subject, context):
    smtp_user = EmailConfig.AWS_SES_SMTP_USER
    smtp_pw = EmailConfig.AWS_SES_SMTP_PASSWORD
    smtp_host = EmailConfig.AWS_SES_SMTP_HOST

    sender = 'info@qll.co'
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver
    msg['Date'] = utils.formatdate(localtime = 1)
    msg['Message-ID'] = utils.make_msgid()
    msg.attach(MIMEText(context, 'html', _charset="UTF-8"))

    s = smtplib.SMTP(smtp_host)
    s.starttls()
    s.login(smtp_user, smtp_pw)
    s.sendmail(sender, receiver, msg.as_string())

@app.task(name='task_queue.qmail.send_email')
def send_email(receiver=None, title='', template_file=None, **kwargs):
    env = Environment(loader=PackageLoader(__name__, 'EmailTemplates'))
    template = env.get_template(template_file)
    context = template.render(title=title, **kwargs)
    _smtp_sendMail(receiver, title, context)





### ETL ###
from elasticsearch import Elasticsearch, helpers
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orm import *
from datetime import timedelta
import redis

engine = create_engine(CeleryConfig.SQLALCHEMY_DATABASE_URI, echo=True)
Session = sessionmaker(bind=engine)
sqlClient = Session()
redisClient = redis.StrictRedis(host=CeleryConfig.REDIS_HOST, port=6379, db=0)
elasticSearchClient = Elasticsearch(CeleryConfig.ELASTICSEARCH_HOST)

@app.task(name='task_queue.classSession_cleanup')
def classSession_cleanup():
    try:
        redisClient.delete(*redisClient.keys('rb.T*'))
    except redis.exceptions.ResponseError: # already empty
        pass
    sqlClient.query(TUser).delete()
    sqlClient.query(Trec).delete()
    sqlClient.commit()


def _toIndexBody(lib_book):
    book = lib_book.book
    index_body = {'_index':'qland',
                  '_type':'book',
                  '_id':lib_book.id,
                  'category': lib_book.category_id,
                  'status': lib_book.status,
                  'recommendation': lib_book.recommendation,
                  'score' : 0 if lib_book.stats == None else round(lib_book.stats[0].score, 1),
                  'likes' : 0 if lib_book.stats == None else lib_book.stats[0].likes,
                  'pageview': 0 if lib_book.stats == None else lib_book.stats[0].pageview,
                  'title': book.title,
                  'author': '' if not book.author else book.author.username,
                  'description': '' if not book.description else book.description,
                  'audience': u'一般大眾' if not book.for_user else book.for_user,
                  'target':   u'增長見聞' if not book.learning_target else book.learning_target,
                  'lang':     u'中文' if not book.lang else book.lang,
                  'duration': '' if not book.duration else book.duration,
                  'createdtime': book.create_datetime
                 }
    return index_body


@app.task(name='task_queue.updateSearchIndex')
def updateSearchIndex(*args, **kwargs):
    pops = {}
    for i in range(0,1000):
        temp = redisClient.spop('search_index')
        if not temp:
            break
        else:
            pops[temp] = ''
    lib_book_ids = pops.keys()
    lib_book_ids = [ int(item) for item in lib_book_ids ]
    lib_books = sqlClient.query(LibraryBook).filter(LibraryBook.id.in_(lib_book_ids)).all()
    books = []
    for lib_book in lib_books:
        books.append( _toIndexBody(lib_book) )
    helpers.bulk(elasticSearchClient, books)


'''app.conf.CELERYBEAT_SCHEDULE = {
    'update_search_index': {
        'task': 'task_queue.updateSearchIndex',
        'schedule': timedelta(minutes=3)
    }
}'''
