import os
import redis
from celery import Celery

class Celery_Config:
    BROKER_URL              = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    CELERY_RESULT_BACKEND   = 'redis://%s:6379/0' % os.environ['REDIS_HOST']
    SQLALCHEMY_DATABASE_URI = os.environ['SQLALCHEMY_DATABASE_URI']
    REDIS_HOST              = os.environ['REDIS_HOST']

app = Celery('tasks')
app.config_from_object(Celery_Config)


## IMPORTANT: move to somewhere else

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
    context = template.render(**kwargs)
    _smtp_sendMail(receiver, title, context)


# IMPORTANT: remove ORM mapping redundancy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, String, Integer, DateTime, ForeignKey, Unicode

Base = declarative_base()
engine = create_engine(Celery_Config.SQLALCHEMY_DATABASE_URI, echo=True)
Session = sessionmaker(bind=engine)

dbSession = Session()
redisClient = redis.StrictRedis(host=Celery_Config.REDIS_HOST, port=6379, db=0)

class Trec(Base):
    __tablename__ = 'trec'

    id = Column(Integer, primary_key=True)
    tcode = Column(String(64))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    lib_book_id = Column(Integer, ForeignKey('library_book.id'))
    owner_id = Column(Integer, ForeignKey('user.id'))
    memo = Column(Text)
    num_tusers = Column(Integer)
    auth_key = Column(String(32))

class TUser(Base):
    __tablename__ = 'tuser'

    id = Column(Integer, primary_key=True)
    join_dtime = Column(DateTime)
    user_nickname = Column(Unicode(64))
    identity = Column(String(32))
    trec_id  = Column(Integer, ForeignKey('trec.id'))
    auth_key = Column(String(32))

@app.task(name='task_queue.classSession_cleanup')
def classSession_cleanup():
    try:
        redisClient.delete(*redisClient.keys('rb.*'))
    except redis.exceptions.ResponseError: # already empty
        pass
    dbSession.query(TUser).delete()
    dbSession.query(Trec).delete()
    dbSession.commit()