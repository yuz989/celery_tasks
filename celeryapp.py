# -.- coding: utf-8 -.-
import sys
import os
sys.path.append(os.getcwd())

import json
import pickle
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
    AWS_SES_SMTP_USER = os.environ['AWS_SES_SMTP_USER']
    AWS_SES_SMTP_PASSWORD = os.environ['AWS_SES_SMTP_PASSWORD']
    AWS_SES_SMTP_HOST = os.environ['AWS_SES_SMTP_HOST']
    AWS_SES_SMTP_PORTS = [25, 465, 587]


def _smtp_sendMail(receiver, subject, context):
    smtp_user = EmailConfig.AWS_SES_SMTP_USER
    smtp_pw = EmailConfig.AWS_SES_SMTP_PASSWORD
    smtp_host = EmailConfig.AWS_SES_SMTP_HOST

    sender = 'info@qll.co'
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver
    msg['Date'] = utils.formatdate(localtime=1)
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
@app.task(name='task_queue.run_upload2ql')
def run_upload2ql(**kwargs):
    # FIX THIS
    api_url = 'http://172.30.1.195/ConverterDev.jsp'

    assert 'id' in kwargs

    encoded = urllib.urlencode(kwargs)

    req = urllib2.Request(api_url, encoded)
    try:
        response = urllib2.urlopen(req)

        if response.code != 200:
            raise Exception(response.msg)

        redisClient.hset(('converter.%s' % kwargs['id']), 'progress', '2')

    except Exception as e:
        redisClient.hset('converter.errors', kwargs['id'], e.message)

### ETL ###
from datetime import datetime, timedelta
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
    except redisException.ResponseError:  # already empty
        pass

    sqlClient.query(TUser).delete()
    sqlClient.query(Trec).delete()
    sqlClient.commit()
    sqlClient.close()


def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
    full_path = os.path.dirname(os.path.realpath(__file__))
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
        pairs = redisClient.zrem_bulk('lib_app_analytics', batch_size)  # FIX THIS: no reliability
        lib_book_ids = {int(item[0]): int(item[1]) for item in pairs}
        lib_book_stats = sqlClient.query(LibraryBookStatistics).filter(
            LibraryBookStatistics.lib_book_id.in_(lib_book_ids.keys())).all()
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

        pagePath = '/portfolio/book/%s' % (lib_book.uri_id)
        try:
            results = service.data().ga().get(
                ids='ga:' + profile_id,
                start_date='2015-07-01',
                end_date='today',
                metrics='ga:pageviews,ga:uniquePageviews',
                dimensions='ga:pagePath',
                filters='ga:pagePath==%s' % pagePath).execute()

            if results['totalResults'] != 0:
                pageview = results.get('rows')[0][1]
                unique_pageview = results.get('rows')[0][2]
            else:
                pageview = unique_pageview = 0

            lib_book.stats[0].pageview = pageview
            lib_book.stats[0].unique_pageview = unique_pageview

        except Exception as e:
            logging.error('[lib_book:pageview:%s]error: %s' % (str(lib_book.id), e.message))

    sqlClient.commit()
    sqlClient.close()
    redisClient.sadd_bulk('search_index', lib_book_ids)

@app.task(name='task_queue.fb_scrape_url')
def fb_scrape_url(url):
    param = {
        'scrape': 'true',
        'id': url,
        'accesstoken': CeleryConfig.FB_API_KEY
    }

    encoded = urllib.urlencode(param)

    req = urllib2.Request('https://graph.facebook.com', encoded)

    try:
        response = urllib2.urlopen(req)
        r = response.read()

        if response.code != 200:
            raise Exception('error: %d, %s' % response.code, response.msg)
    except Exception as e:
        print e.message


@app.task(name='task_queue.updateSingleLibBookPageView')
def updateSingleLibBookPageView(uri_id):
    engine = create_engine(CeleryConfig.SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    sqlClient = Session()

    lib_book = sqlClient.query(LibraryBook).filter(LibraryBook.uri_id==uri_id).first()

    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    service_account_email = CeleryConfig.GOOGLE_SERVICE_ACCOUNT_EMAIL
    key_file_location = CeleryConfig.GOOGLE_SERVICE_CREDENTIAL_PATH
    service = get_service('analytics', 'v3', scope, key_file_location,
                          service_account_email)
    profile_id = get_first_profile_id(service)

    pagePath = '/portfolio/book/%s' % (lib_book.uri_id)
    try:
        results = service.data().ga().get(
            ids='ga:' + profile_id,
            start_date='2015-07-01',
            end_date='today',
            metrics='ga:pageviews,ga:uniquePageviews',
            dimensions='ga:pagePath',
            filters='ga:pagePath==%s' % pagePath).execute()

        if results['totalResults'] != 0:
            pageview = results.get('rows')[0][1]
            unique_pageview = results.get('rows')[0][2]
        else:
            pageview = unique_pageview = 0

        lib_book.stats[0].pageview = pageview
        lib_book.stats[0].unique_pageview = unique_pageview

    except Exception as e:
        logging.error('[lib_book:pageview:%s]error: %s' % (str(lib_book.id), e.message))

    sqlClient.commit()
    sqlClient.close()

def _toIndexBody(lib_book):
    book = lib_book.book
    index_body = {'_index': 'qland',
                  '_type': 'book',
                  '_id': lib_book.id,
                  'category': lib_book.category_id,
                  'status': lib_book.status,
                  'recommendation': lib_book.recommendation,
                  'score': 0 if lib_book.stats == None else round(lib_book.stats[0].score, 1),
                  'likes': 0 if lib_book.stats == None else lib_book.stats[0].likes,
                  'pageview': 0 if lib_book.stats == None else lib_book.stats[0].pageview + lib_book.stats[
                      0].app_pageview,
                  'title': book.title,
                  'author': '' if not book.author else book.author.username,
                  'description': '' if not book.description else book.description,
                  'audience': u'一般大眾' if not book.for_user else book.for_user,
                  'target': u'增長見聞' if not book.learning_target else book.learning_target,
                  'lang': u'中文' if not book.lang else book.lang,
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
        lib_book_ids = [int(item) for item in lib_book_ids]
        lib_books = sqlClient.query(LibraryBook).filter(LibraryBook.id.in_(lib_book_ids)).all()
        books = []
        for lib_book in lib_books:
            books.append(_toIndexBody(lib_book))
        helpers.bulk(elasticSearchClient, books)

    finally:
        sqlClient.close()

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

def _get_aws_client(service, region='ap-southeast-1', _access_key=None, _secret_key=None):
    import boto3

    access_key = _access_key or CeleryConfig.AWS_ACCESS_KEY
    secret_key = _secret_key or CeleryConfig.AWS_SECRET_KEY
    auth = {"region_name": region, "aws_access_key_id": access_key, "aws_secret_access_key": secret_key}
    return boto3.client(service, **auth)

@app.task(name='task_queue.exportQLectureAnswers')
def exportQLectureAnswers(trec_id, tcode):
    def qlectureRedisKey(tcode):
        return {
            'WILDCARD': 'rb.' + tcode + '.*',
            'CLASS_INFO': 'rb.' + tcode + '.info',
            'CONTENT': 'rb.' + tcode + '.content',
            'USERPROFILE': 'rb.' + tcode + '.profile',
            'ONLINES': 'rb.' + tcode + '.online',
            'STATS': 'rb.' + tcode + '.stats',
            'PREFIX_ROLLBOOK': 'rb.' + tcode + '.users.',
            'WILDCARD_USER': 'rb.' + tcode + '.user.*',
            'TMP': 'rb.' + tcode + '.tmp',
            'CHANNEL': 'rb.' + tcode + '.channel'
        }

    def _delete_All_keys(pattern):
        keys = redisClient.keys(pattern)
        pipeline = redisClient.pipeline()
        for key in keys:
            pipeline.delete(key)
        pipeline.execute()

    questionType = {
        1: 'match',
        2: 'choose',
        3: 'true_false',
        4: 'sentence',
        5: 'vocabulary',
        6: 'cloze',
        7: 'comprehension'
    }

    keys = qlectureRedisKey(tcode)
    current_time = str(datetime.utcnow())[0:19]
    classInfo = redisClient.hgetall(keys['CLASS_INFO'])
    numUsers = classInfo.get('num_users') or 0
    content = redisClient.hgetall(keys['CONTENT'])

    if numUsers == 0 or not content:
        _delete_All_keys(keys['WILDCARD'])
        return False
    else:
        try:
            tests = json.loads(content['dc'])

            fireHoseClient = _get_aws_client('firehose', region='us-west-2')

            key_users = redisClient.keys(keys['WILDCARD_USER'])

            for page, page_meta in tests.items():

                if page_meta['type'] == 7:
                    continue

                for key_user in key_users:

                    tuser_id = key_user.split('.')[-1]

                    answer = redisClient.hget(key_user, page)

                    if answer:
                        d = [trec_id, tuser_id, page, answer, current_time]

                        data = '|'.join(d) + '\n'
                        fireHoseClient.put_record(
                            DeliveryStreamName='qlecture-log-answer',
                            Record={
                                'Data': data
                            })

            _delete_All_keys(keys['WILDCARD'])
            return True

        except:
            _delete_All_keys(keys['WILDCARD'])
            return False

@app.task(name='task_queue.unloadToS3')
def unloadToS3(trec_id, tcode, num_page):
    Session = sessionmaker(bind=create_engine(CeleryConfig.SQLALCHEMY_DATABASE_URI))
    sqlClient = Session()
    trec = sqlClient.query(Trec).filter(Trec.id==trec_id).first()

    engine = sqlalchemy.create_engine(CeleryConfig.REDSHIFT_CONNECTION_STRING)
    try:
        s3_object_path = 's3://qlecture-download/%d/' % trec_id

        field = ', '.join(map(lambda i: 'f.p' + str(i), range(1, num_page + 1)))
        sub_field = ', '.join(
            map(lambda j: ('SUM( case when page=%d then secs else 0 end ) as p%d' % (j, j)), range(1, num_page + 1)))

        query = '''
                SELECT t1.*, t2.login_time, t2.logout_time, t2.num_logouts FROM
                (
                    SELECT u.tuser_id, u.name, %s
                    FROM
                    (
                        SELECT tuser_id,

                        %s

                        FROM
                        (SELECT tuser_id, page, datediff(secs, from_dtime, to_dtime) as secs

                        FROM
                        (SELECT tuser_id, page, min(from_dtime) as from_dtime, min(to_dtime) as to_dtime
                         FROM qlecture_presence WHERE trec_id = %d
                         group by tuser_id, page
                        )
                    )
                    group by tuser_id
                )f
                INNER JOIN qlecture_user u
                on u.tuser_id = f.tuser_id
            )t1

            INNER JOIN
            (
                SELECT tuser_id,
                       min( case when action=\\'login\\' then dtime end) as login_time,
                       max( case when action=\\'logout\\' then dtime end) as logout_time,
                       count( case when action=\\'logout\\' then 1 else 0 end ) as num_logouts

                FROM qlecture_login

                WHERE trec_id=%d

                group by tuser_id
            )t2

            on t1.tuser_id = t2.tuser_id
        ''' % (field, sub_field, trec_id, trec_id)

        if trec.test_content:
            ans_pages = pickle.loads(trec.test_content)
            ans_pages = ans_pages.keys()

            ans_field = ', '.join(
                map(lambda p: ("MAX(CASE WHEN page = %s THEN answer ELSE \\'NULL\\' END) AS ans%s " % (p, p)), ans_pages))

            answer_query = '''
                SELECT

                tuser_id,

                %s

                From qlecture_answer

                where trec_id = %d

                group by tuser_id
            ''' % (ans_field, trec_id)

            page_field = ', '.join(map(lambda i: 't.p' + str(i), range(1, num_page + 1)))
            ans_page_field = ', '.join(map(lambda i: 't3.ans' + str(i), ans_pages))

            tmp_field = 't.name, %s, t.login_time, t.logout_time, t.num_logouts, %s' % (page_field, ans_page_field)

            query = 'SELECT %s FROM (%s)t LEFT JOIN (%s)t3 on t.tuser_id = t3.tuser_id' % (tmp_field, query, answer_query)

        query = ''' unload (' %s ')
            to '%s' with credentials as 'aws_access_key_id=%s;aws_secret_access_key=%s' PARALLEL OFF DELIMITER ','
        ''' % (query, s3_object_path, CeleryConfig.AWS_ACCESS_KEY, CeleryConfig.AWS_SECRET_KEY)

        try:
            engine.execute(query)
        except:
            return

        trec.status = 'S'
        trec.report_location = s3_object_path
        sqlClient.commit()
        sqlClient.close()
    except:
        pass

@app.task(name='task_queue.unloadQlectureStatistics')
def unloadQlectureStatistics(trec_id, tcode, num_page):
    halfway_to_redshift = exportQLectureAnswers(str(trec_id), tcode)

    if halfway_to_redshift: # wait for s3 buffer flushing
        unloadToS3.apply_async(countdown=60, kwargs={
            'trec_id': trec_id,
            'tcode': tcode,
            'num_page': num_page
        })
    else: # unload immediately
        unloadToS3(
            trec_id=trec_id,
            tcode=tcode,
            num_page=num_page
        )


@app.task(name='task_queue.appMessengerNotification')
def appMessengerNotification():
    from aws_service import AWSService

    engine = create_engine(CeleryConfig.SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    sqlClient = Session()

    messenger_owners =sqlClient.query(AppMessengerAccount).filter(AppMessengerAccount.device_token_id!=None).filter(AppMessengerAccount.is_owner==True).all()

    for messenger_owner in messenger_owners:
        device_token     = sqlClient.query(DeviceToken).filter(DeviceToken.id==messenger_owner.device_token_id).first()
        roster_list_item = sqlClient.query(AppMessengerAccountRosterList).filter(AppMessengerAccountRosterList.messenger_id==messenger_owner.id, AppMessengerAccountRosterList.num_unreads>0).first()
        if roster_list_item:
            data = u'Shirley: [%s] 有新訊息囉,快點去瞧瞧吧~~~' % messenger_owner.app.title
            aws_service = AWSService(CeleryConfig.AWS_ACCESS_KEY, CeleryConfig.AWS_SECRET_KEY)
            aws_service.send_sns(data, 'endpoint', arn=device_token.sns_endpoint)

            #HOTFIX: remove this!
            data = u'Lulu: [%s] 有新訊息囉,趕快帶 malu 去瞧瞧吧~~~' % messenger_owner.app.title
            aws_service.send_sns(data, 'endpoint', arn='arn:aws:sns:ap-southeast-1:362048893305:endpoint/APNS/Wenzaoapp1/b99c20f0-876c-34b0-986f-ec22f809d344')

app.conf.CELERYBEAT_SCHEDULE = {
    'update_pageview': {
        'task': 'task_queue.updateLibBookPageView',
        'schedule': timedelta(minutes=5)
    },
    'app_messenger_notification': {
        'task': 'task_queue.appMessengerNotification',
        'schedule': timedelta(hours=1)
    }
}

