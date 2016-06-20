#!/usr/bin python
# -.- coding: utf-8 -.-
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import json
import os
import sqlalchemy
from celery import Celery
from celeryconfig import CeleryConfig

app = Celery('tasks')
app.config_from_object(CeleryConfig)


### EMail ###
import urllib2, urllib
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


### QLecture ###
@app.task(name='task_queue.run_ppt2ql')
def run_ppt2ql(job_id, file_name, url):
    
    api_url = 'http://172.30.1.195/Converter.jsp'

    data = urllib.urlencode({'id': job_id, 'file name': file_name, 'url':url})

    req = urllib2.Request(api_url, data)
    try:
        response = urllib2.urlopen(req)

        if response.code != 200:
            raise Exception(response.msg)

        redisClient.hset( ('ppt2ql.%s' % job_id), 'progress', '2')

    except Exception as e:
        redisClient.hset('ppt2ql.errors', job_id, e.message)


### ETL ###
from datetime import timedelta
import logging
import csv

from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
import httplib2

from elasticsearch import Elasticsearch, helpers

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orm import *

from redisUtil import RedisClient
from redis import exceptions as redisException

redisClient = RedisClient(host=CeleryConfig.REDIS_HOST, port=6379, db=0)
elasticSearchClient = Elasticsearch(CeleryConfig.ELASTICSEARCH_HOST)

@app.task(name='task_queue.classSession_cleanup')
def classSession_cleanup():
    engine = create_engine(CeleryConfig.SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    sqlClient = Session()

    try:
        redisClient.delete(*redisClient.keys('rb.T*'))
    except redisException.ResponseError: # already empty
        pass

    sqlClient.query(TUser).delete()
    sqlClient.query(Trec).delete()
    sqlClient.commit()
    sqlClient.close()

def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):

    full_path = os.path.dirname( os.path.realpath(__file__) )
    path_to_client_secret = full_path + key_file_location
    f = open(path_to_client_secret, 'rb')
    key = f.read()
    f.close()

    credentials = SignedJwtAssertionCredentials(service_account_email, key,
                                                scope=scope)

    http = credentials.authorize(httplib2.Http())

    service = build(api_name, api_version, http=http)

    return service

def get_first_profile_id(service):

    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        # Get the first Google Analytics account.
        account = accounts.get('items')[0].get('id')

        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
            accountId=account).execute()

        if properties.get('items'):
            # Get the first property id.
            property = properties.get('items')[0].get('id')

            # Get a list of all views (profiles) for the first property.
            profiles = service.management().profiles().list(
                accountId=account,
                webPropertyId=property).execute()

            if profiles.get('items'):
                # return the first view (profile) id.
                return profiles.get('items')[0].get('id')

    return None

@app.task(name='task_queue.updateLibBookPageView')
def updateLibBookPageView():
    engine = create_engine(CeleryConfig.SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    sqlClient = Session()

    batch_size = 500

    # aggregate lib_app_pageview
    if redisClient.zcard('lib_app_analytics') != 0:
        pairs = redisClient.zrem_bulk('lib_app_analytics', batch_size) #IMPORTANT: Does not provide reliability
        lib_book_ids = {int(item[0]) : int(item[1]) for item in pairs}
        lib_book_stats = sqlClient.query(LibraryBookStatistics).filter(LibraryBookStatistics.lib_book_id.in_(lib_book_ids.keys())).all()
        for lib_book_stat in lib_book_stats:
            lib_book_stat.app_pageview += lib_book_ids.get(lib_book_stat.lib_book_id, 0)

        sqlClient.commit()

    # aggregate lib_page_view
    if redisClient.scard('lib_analytics') == 0:
        sqlClient.close()
        return

    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    service_account_email = CeleryConfig.GOOGLE_SERVICE_ACCOUNT_EMAIL
    key_file_location = CeleryConfig.GOOGLE_SERVICE_CREDENTIAL_PATH
    service = get_service('analytics', 'v3', scope, key_file_location,
              service_account_email)
    profile_id = get_first_profile_id(service)

    lib_book_ids = redisClient.spop_bulk('lib_analytics', batch_size)
    lib_books = sqlClient.query(LibraryBook).filter(LibraryBook.id.in_(lib_book_ids)).all()

    for lib_book in lib_books:

        pagePath = '/portfolio/book/%s' % ( lib_book.uri_id )
        try:
            results =  service.data().ga().get(
                ids='ga:' + profile_id,
                start_date='2015-07-01',
                end_date='today',
                metrics='ga:pageviews',
                dimensions='ga:pagePath',
                filters='ga:pagePath==%s' % pagePath).execute()

            if results['totalResults'] != 0:
                pageview = results.get('rows')[0][1]
            else:
                pageview = 0

            lib_book.stats[0].pageview = pageview

        except Exception as e:
            logging.error('[lib_book:pageview:%s]error: %s' % (str(lib_book.id), e.message) )

    sqlClient.commit()
    sqlClient.close()
    redisClient.sadd_bulk('search_index', lib_book_ids)

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
                  'pageview': 0 if lib_book.stats == None else lib_book.stats[0].pageview + lib_book.stats[0].app_pageview,
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
    engine = create_engine(CeleryConfig.SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    sqlClient = Session()

    lib_book_ids = redisClient.spop_bulk('search_index', 1000)

    if len(lib_book_ids) == 0:
        return

    try:
        lib_book_ids = [ int(item) for item in lib_book_ids ]
        lib_books = sqlClient.query(LibraryBook).filter(LibraryBook.id.in_(lib_book_ids)).all()
        books = []
        for lib_book in lib_books:
            books.append( _toIndexBody(lib_book) )
        helpers.bulk(elasticSearchClient, books)
        sqlClient.close()
    except:
        pass


# remove this !
from boto.s3.connection import S3Connection

def upload_file(fname, data, dst_dir='', type='text'):
    conn = S3Connection(os.environ['AWS_ACCESS_KEY'], os.environ['AWS_SECRET_KEY'])
    bucket = conn.get_bucket('qllco')
    key_name = os.path.join(os.path.join(dst_dir, fname))
    key = bucket.new_key(key_name)

    if type == 'text':
        key.set_contents_from_string(data)
    elif type == 'file':
        key.set_contents_from_file(data)

    key.set_acl('public-read')
    url = key.generate_url(expires_in=0, query_auth=False)
    return url


@app.task(name='task_queue.transformQlectureLog')
def exportQLectureLog(tcode):
    engine = create_engine(CeleryConfig.SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    sqlClient = Session()

    def qlectureRedisKey(tcode):
        return {
            'CLASS_INFO'      : 'rb.' + tcode + '.info',
            'TEST_CONTENT'    : 'test.' + tcode,
            'USERPROFILE'     : 'rb.' + tcode + '.profile',
            'ONLINES'         : 'rb.' + tcode + '.online',
            'STATS'           : 'rb.' + tcode + '.stats',
            'PREFIX_ROLLBOOK' : 'rb.' + tcode + '.users.',
            'PREFIX_USER'     : 'rb.' + tcode + '.',
            'TMP'             : 'rb.' + tcode + '.tmp',
            'CHANNEL'         : 'rb.' + tcode + '.channel'
        }

    questionType = {
        1: 'match',
        2: 'choose',
        3: 'true_false',
        4: 'sentence',
        5: 'vocabulary',
        6: 'cloze',
        7: 'comprehension'
    }

    trec = sqlClient.query(Trec).filter(Trec.tcode==tcode).first()
    keys = qlectureRedisKey(tcode)

    classInfo   = redisClient.hgetall(keys['CLASS_INFO'])
    numUsers    = classInfo.get('num_users')

    if not numUsers:
        return

    else:

        numUsers         = int(numUsers)
        numRollBookPages = numUsers /32 + 1
        fileName         = ( '%d.%s.csv' % (trec.id, trec.tcode) )

        with open(fileName, 'w+') as csvfile:
            try:
                test_content = json.loads(redisClient.hgetall(keys['TEST_CONTENT'])['dc'])

                writer = csv.writer(csvfile)
                writer.writerow(['user_id', 'page', 'type', 'option'])

                for rollBookPage in range(1, numRollBookPages+1):

                    userIDs = redisClient.zrange(keys['PREFIX_ROLLBOOK'] + str(rollBookPage), 0, -1)

                    for userID in userIDs:

                        userAnswers = redisClient.hgetall(keys['PREFIX_USER'] + userID)

                        for pageNumber in userAnswers:
                            try:
                                type = questionType[test_content[pageNumber]['type']]
                                writer.writerow([userID, pageNumber, type, userAnswers[pageNumber] ])
                            except:
                                continue
                csvfile.seek(0)
                upload_file(fname=fileName, data=csvfile, dst_dir='qlecture_csv', type='file')

                trec.status   = 'S'
                sqlClient.commit()
                sqlClient.close()

            finally:
                os.remove(csvfile.name)


@app.task(name='task_queue.exportQlecturePresence')
def exportQlecturePresence():
    engine = sqlalchemy.create_engine(CeleryConfig.REDSHIFT_CONNECTION_STRING)
    r = engine.execute('SELECT * FROM qlecture_presence')

    names = []
    for row in r:
        names.append(row)

@app.task(name='task_queue.unloadQlectureStatistics')
def unloadQlectureStatistics():
    return

app.conf.CELERYBEAT_SCHEDULE = {
    'update_pageview' : {
        'task': 'task_queue.updateLibBookPageView',
        'schedule': timedelta(minutes=5)
    },
    'update_search_index': {
        'task': 'task_queue.updateSearchIndex',
        'schedule': timedelta(minutes=3)
    }
}

