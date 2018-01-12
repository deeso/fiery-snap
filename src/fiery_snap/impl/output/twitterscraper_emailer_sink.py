from fiery_snap.impl.util.page import Page, ConsumePage, \
               JsonUploadPage, TestPage, EmailInfoPage, MongoSearchPage

from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message
from fiery_snap.utils import parsedate_to_datetime
from fiery_snap.impl.util.simplified_email import SendEmail
from fiery_snap.impl.util.mongo_client_impl import MongoClientImpl
import threading
import web
import json
from pymongo import MongoClient
import logging
import sys
import time

#Pull defanged or all analytics from the working Mongo DBs
#Email results to provided individuals
#update timestamp of last read in mongo db
TAGS = 'tags'
KEYWORDS = 'keywords'
HASHES = 'hashes'
DOMAINS = 'domains'
EMAILS = 'emails'
URLS = 'urls'
PURLS = 'pot_urls'
IPS = 'ips'
CONTENT_ARTIFACTS = 'content_artifacts'

DF_DOMAINS = 'defanged_domains'
DF_EMAILS = 'defanged_emails'
DF_URLS = 'defanged_links'
DF_IPS = 'defanged_ips'

REPORT_ITEMS = [HASHES, DOMAINS, EMAILS, URLS, PURLS, IPS, CONTENT_ARTIFACTS]

class TwitterScraperEmailUpdates(IOBase):
    KEY = 'twitterscraper-email-results'
    DEFAULT_DB = 'mailer-state'
    DEFAULT_COLLECTION = 'info'
    MONGO_URI = 'mongodb://0.0.0.0:27017'
    EMAILER_STATE_DBNAME = 'email'
    EMAILER_STATE_COLNAME = 'state'
    EMAILER_SUBJECT = '[Fiery-Snap] Twitter Feed Results'
    MONGO_RESULT_DBNAME = 'twitter-feeds'
    MONGO_RESULT_COLNAME = 'processed-tweets'
    MONGO_STATE_DBNAME = 'emailer'
    MONGO_STATE_COLNAME = 'state'
    MONGO_RESULT_CONFIG = {
                            'name': 'mongo-results',
                            'uri': MONGO_URI,
                            'dbname': MONGO_RESULT_DBNAME,
                            'colname': MONGO_RESULT_COLNAME,
                            }
    EMAILER_STATE_CONFIG = {
                            'name': 'emailer-state',
                            'uri': MONGO_URI,
                            'dbname': EMAILER_STATE_DBNAME,
                            'colname': EMAILER_STATE_COLNAME,
                            }
    REQUIRED_CONFIG_PARAMS = ['name', ]
    OPTIONAL_CONFIG_PARAMS = [['publishers', {}],
                              ['subscribers', {}],
                              ['mongo_result_config', MONGO_RESULT_CONFIG],
                              ['mongo_state_config', EMAILER_STATE_CONFIG],
                              ['start_timestamp', None], # send reports out every X minutes
                              ['sleep_time', 5.0*60], # send reports out every X minutes
                              ['recipients', []],
                              ['sender_details', None],
                             ]

    def __init__(self, config_dict, add_local=False):
        pages = [TestPage, JsonUploadPage,
                 MongoSearchPage, ConsumePage, EmailInfoPage]
        super(TwitterScraperEmailUpdates, self).__init__(config_dict, pages=pages)
        self.output_queue = []
        rs = self.random_name(prepend=self.key)
        self.name = self.config.get('name', rs)
        self.uri = self.config.get('uri')
        self.mongo_result_config = self.config.get('mongo_result_config')
        self.mongo_state_config = self.config.get('mongo_state_config')
        self.results_db_conn = self.new_result_client()
        self.state_db_conn = self.new_state_client()
        self.state_dbname = self.mongo_state_config['dbname']
        self.state_colname = self.mongo_state_config['colname']
        self.sender_details = self.config.get('sender_details', None)
        self.recipients = self.config.get('recipients', [])
        if self.sender_details is None:
            raise Exception("Missing the sender details")
        self.init_timestamp = self.config.get('start_timestamp', None)
        self.initted = False

    def reset(self, dbname=None, colname=None):
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        logging.debug("resetting %s[%s]" % (dbname,colname))
        return self.db_conn.reset(dbname=dbname, colname=colname)

    def reset_all(self, dbname=None, colname=None):
        super(MongoStore, self).reset_all()
        self.reset()
        return True

    def init_state(self, timestamp=None):
        logging.debug("Initializing state with timestamp: %s"%(str(timestamp)))
        logging.debug("Initializing Mongo State with %s[%s]['timestamp'] = : %s"%(self.state_dbname, self.state_colname, str(timestamp)))
        state = self.state_db_conn.get_one(dbname=self.state_dbname, colname=self.state_colname, obj_dict={})
        if state is None:
            # do other stuff here
            state = {'obtained_timestamp': timestamp}
            _id = None
            self.state_db_conn.inserts([state,],
                                      dbname=self.state_dbname,
                                      colname=self.state_colname)
        elif 'obtained_timestamp' not in state:
            state['obtained_timestamp'] = timestamp
            self.state_db_conn.inserts([state,],
                                      dbname=self.state_dbname,
                                      colname=self.state_colname,
                                      update=True,)
        return state

    def reset_all(self, dbname=None, colname=None):
        self.reset(dbname=dbname, colname=colname)

    def reset(self, dbname=None, colname=None):
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        logging.debug("resetting state %s[%s]" % (dbname,colname))
        return self.state_db_conn.reset(dbname=dbname, colname=colname)

    def get_last_consumed_timestamp(self):
        state = self.state_db_conn.get_one(dbname=self.state_dbname, colname=self.state_colname, obj_dict={})
        if state is not None:
            return state.get('obtained_timestamp', None)
        return None

    def set_last_consumed_timestamp(self, timestamp):
        logging.debug("Setting timestamp from last report result: %s"%timestamp)
        state = self.state_db_conn.get_one(dbname=self.state_dbname, colname=self.state_colname, obj_dict={})
        if state is None:
            logging.debug("Empty state? initializing it with current timestamp")
            return self.init_state(timestamp=timestamp)
        else:
            state['obtained_timestamp'] = timestamp
            self.state_db_conn.insert(state,
                                      dbname=self.state_dbname,
                                      colname=self.state_colname,
                                      update=True,
                                      _id=str(state['_id']))
        return state

    def consume_and_publish(self):
        if not self.initted:
            self.init_state(timestamp=self.init_timestamp)

        logging.debug("Getting state and extracting timestamp")
        timestamp = self.get_last_consumed_timestamp()
        logging.debug("Last time analytics were pulled: %s"%timestamp)
        results = self.pull_analytics(timestamp)
        logging.debug("Pulled the analytics and found %d records to report about" %(len(results)))

        timestamps = [v['obtained_timestamp'] for v in results]
        timestamps = sorted(timestamps)
        last_timestamp = timestamps[-1] if len(timestamps) > 0 else None
        if last_timestamp is None or len(results) == 0:
            logging.debug("Nothing to report")
            return {'result': {},
                    'reports_by_user': {},
                    'timestamp': timestamp,
                    }
        logging.debug("First timestamp in this batch: %s" %(timestamps[0]))
        logging.debug("Last timestamp in this batch: %s" %(last_timestamp))
        logging.debug("Size of this batch: %s" %(len(timestamps)))
        self.set_last_consumed_timestamp(last_timestamp)

        reports_by_user = self.aggregate_results_by_user(results)
        logging.debug("Performing an email of the reports")
        if timestamp is None:
            timestamp = timestamps[0]
        r = self.email_analytics(reports_by_user, timestamp)
        return {'result': r,
                'reports_by_user': reports_by_user,
                'timestamp': timestamp,
                }

    def new_state_client(self):
        client_params = {}
        client_params['id_keys'] = ['_id',]
        client_params['dbname'] = self.mongo_state_config['dbname']
        client_params['colname'] = self.mongo_state_config['colname']
        client_params['uri'] = self.mongo_state_config['uri']
        client_params['name'] = self.mongo_state_config['name']
        s = '{name}: {uri} {dbname}[{colname}]'.format(**client_params)
        logging.debug("Initializing MongoClient to: %s"%(s))
        return MongoClientImpl(**client_params)

    def new_result_client(self):
        client_params = {}
        client_params['dbname'] = self.mongo_result_config['dbname']
        client_params['colname'] = self.mongo_result_config['colname']
        client_params['uri'] = self.mongo_result_config['uri']
        client_params['name'] = self.mongo_result_config['name']
        s = '{name}: {uri} {dbname}[{colname}]'.format(**client_params)
        logging.debug("Initializing MongoClient to: %s"%(s))
        return MongoClientImpl(**client_params)

    def handle(self, path, data):
        logging.debug("handling request (%s)" % (path))
        results = {'msg': 'Not a valid command: %s' % (path)}
        if path == TestPage.NAME:
            return {'msg': 'success'}
        if path == ConsumePage.NAME:
            return results
        elif path == EmailInfoPage.NAME:
            command = data['command'] if 'command' in data else None
            if command == 'consume':
                results = self.consume_and_publish()
            else:
                results = {'msg': 'Not a valid command: %s["%s"]' % (path, command)}
            return results
        elif path == JsonUploadPage.NAME:
            return {}
        return {'error': 'unable to handle message type: %s' % path}

    @classmethod
    def parse(cls, config_dict):
        new_config_dict = {}
        new_config_dict['name'] = config_dict.get('name')
        new_config_dict['start_timestamp'] = config_dict.get('start_timestamp', None)
        new_config_dict['service'] = config_dict.get('service')
        mongo_result_config = {}

        mrc = config_dict.get('mongo-result-config', None)
        if mrc is not None:
            new_config_dict['mongo_result_config'] = mrc
        
        msc = config_dict.get('mongo-state-config', None)
        if msc is not None:
            new_config_dict['mongo_state_config'] = msc

        new_config_dict['publishers'] = config_dict.get('publishers', {})
        new_config_dict['subscribers'] = config_dict.get('subscribers', {})
        new_config_dict['dbname'] = config_dict.get('dbname', cls.DEFAULT_DB)
        new_config_dict['colname'] = config_dict.get('colname', cls.DEFAULT_COLLECTION)
        new_config_dict['sleep_time'] = config_dict.get('sleep_time', 30.0)
        new_config_dict['recipients'] = []
        for r in config_dict.get('recipients', {}).values():
            # name, email
            if isinstance(r, list) and len(r) == 2:
                d = {'name': r[0], 'email': r[1]}
                new_config_dict['recipients'].append(r)
            elif isinstance(r, dict) and 'name' in r and 'email' in r:
                new_config_dict['recipients'].append(r)
            elif isinstance(r, list):
                raise Exception("Expecting list containing ['name', 'email']")
            elif isinstance(r, dict):
                raise Exception("Expecting list containing {'name':name, 'email':email}")
            else:
                raise Exception("Expecting list or dict: ['name', 'email'] or {'name':name, 'email':email}")

        if len(new_config_dict['recipients']) == 0:
            raise Exception("Email recipients must be specified")

        sender_details = {}
        sender_info = config_dict.get('sender-details', {})
        sender_details['name'] = sender_info.get('name', None)
        sender_details['email'] = sender_info.get('email', None)
        sender_details['password'] = sender_info.get('password', None)
        sender_details['subject'] = sender_info.get('subject', cls.EMAILER_SUBJECT)
        sender_details['server'] = sender_info.get('server', None)
        sender_details['port'] = sender_info.get('port', None)
        sender_details['tls'] = sender_info.get('tls', True)

        # if sender_details['name'] is None or \
        #    sender_details['email'] is None or \
        #    sender_details['password'] is None:
        #    raise Exception("Missing sender credentials: provide an: email, name, and password")
        # elif sender_details['server'] is None or \
        #    sender_details['host'] is None:
        #    raise Exception("Missing email server parameters provide a: server and port")

        new_config_dict['sender_details'] = sender_details
        return cls(new_config_dict)


    def pull_analytics(self, timestamp=None):
        query = {}
        # FIXME potential for missing sent tweets
        if timestamp is not None:
            query = {'obtained_timestamp': {'$gt': timestamp}}

        # create query based on state
        results = self.results_db_conn.get_all(obj_dict=query)
        return results

    def aggregate_results_by_user(self, results):
        # create report
        items = [HASHES, DOMAINS, EMAILS, URLS, IPS]
        reports = {}
        reports_by_user = {}
        for r in results:
            tm_id = r['tm_id']
            tags = r[TAGS]
            keywords = r[KEYWORDS]
            reports = self.update_with_defanged_entities_content(r, tm_id, reports)
            user = reports[tm_id]['user']
            if user not in reports_by_user:
                reports_by_user[user] = {}
            n_rec = {KEYWORDS: keywords, TAGS: tags}
            reports_by_user[user][tm_id] = n_rec
            for k, v in reports[tm_id].items():
                if k == 'user':
                    continue
                n_rec[k] = v
        return reports_by_user

    def extract_linked_content(self, record, tm_id, reports):

        linked_content = record['linked_content']
        for ci in linked_content:
            if CONTENT_ARTIFACTS not in ci:
                continue
            _df_domains = ci[CONTENT_ARTIFACTS][DF_DOMAINS]
            _df_ips = ci[CONTENT_ARTIFACTS][DF_IPS]
            _df_links = ci[CONTENT_ARTIFACTS][DF_LINKS]
            records[tm_id][DOMAINS] = records[tm_id][DOMAINS] + _df_domains
            records[tm_id][URLS] = records[tm_id][URLS] + _df_links
            records[tm_id][IPS] = records[tm_id][IPS] + _df_ips
        return records


    def extract_entity_item(self, item_name, record, tm_id, reports):
        r = record
        e = r['defanged_entities']
        e2 = r['entities']
        items = e.get(item_name, []) + e2.get(item_name, [])
        items = sorted(set(items))
        reports[tm_id][item_name] = items
        return reports

    def update_with_defanged_entities_content(self, record, tm_id, reports):
        r = record
        items = [HASHES, DOMAINS, EMAILS, URLS, IPS]
        reports[tm_id] = dict([(i, {}) for i in items])
        reports[tm_id]['content'] = r['content']
        reports[tm_id]['user'] = r['user']
        reports[tm_id]['obtained_timestamp'] = r.get('obtained_timestamp', '')
        reports[tm_id]['timestamp'] = r.get('timestamp', '')

        for item_name in items:
            reports = self.extract_entity_item(item_name, record, tm_id, reports)

        reports = self.filter_twitter_urls(record, tm_id, reports)
        return reports


    def filter_twitter_urls(self, record, tm_id, reports):
        reports[tm_id][URLS] = sorted([i for i in reports[tm_id][URLS] if i.strip().find('https://t.co/') != 0])
        reports[tm_id][URLS] = sorted([i for i in reports[tm_id][URLS] if i.strip().find('http://t.co/') != 0])
        reports[tm_id][URLS] = sorted([i for i in reports[tm_id][URLS] if i.strip().find('t.co/') != 0])
        return reports

    def defang(self, list_content):
        #encode_all = lambda x: [i.encode('utf-8') for i in x]
        #defang = lambda l: u", ".join([i.replace(u'.', u'[.]').encode('utf-8') for i in encode_all(l)])
        nl = u''
        failed = False
        for i in list_content:
            t = i
            try:
                i = i.replace(u'.', u'[.]')
                t = i.encode('utf-8')
            except:
                failed = True
                logging.debug("Failed to encode string URL to utf-8")
                continue

            if len(nl) > 0:
                try:
                    nl = nl + u', '+ t.encode('utf-8')
                except:
                    logging.debug("Failed to encode string URL to utf-8")
                    continue
            else:
                try:
                    nl = t.encode('utf-8')
                except:
                    logging.debug("Failed to encode string URL to utf-8")
                    continue
                    
        return nl
            
    def format_text_content(self, reports_by_user):
        lines = []
        twitter_url = lambda tm_id: u"https://twitter.com/i/web/status/"+str(tm_id).encode('utf-8')
        startheader = u'==== Start {} Section ===='
        endheader = u'==== End {} Section ===='
        tweet_header = u'|===     [{}] {}'
        for user, user_recs in reports_by_user.items():
            __lines = []
            __lines.append(startheader.format(user.encode('utf-8')))
            for tm_id, r in user_recs.items():
                add_tags_kws = False
                timestamp = r['timestamp']
                _lines = [tweet_header.format(timestamp.encode('utf-8'), twitter_url(tm_id))]
                if len(r[HASHES]) > 0:
                    _lines.append(u"|========     hashes: {}".format(self.defang(r[HASHES])))
                    add_tags_kws = True
                if len(r[DOMAINS]) > 0:
                    _lines.append(u"|========     domains: {}".format(self.defang(r[DOMAINS])))
                    add_tags_kws = True
                if len(r[URLS]) > 0:
                    _lines.append(u"|========     urls: {}".format(self.defang(r[URLS])))
                    add_tags_kws = True
                if len(r[IPS]) > 0:
                    _lines.append(u"|========     ips: {}".format(self.defang(r[IPS])))
                    add_tags_kws = True
                if len(r[EMAILS]) > 0:
                    _lines.append(u"|========     emails: {}".format(self.defang(r[EMAILS])))
                    add_tags_kws = True
                if len(r[TAGS]) > 0 and add_tags_kws:
                    _lines.append(u"|========     tags: {}".format(self.defang(r[TAGS])))
                if len(r[KEYWORDS]) > 0 and add_tags_kws:
                    _lines.append(u"|========     keywords: {}".format(self.defang(r[KEYWORDS])))

                if len(_lines) > 1:
                    __lines = __lines + _lines + ['|',]
            if len(__lines) == 1:
                continue
            __lines.append(endheader.format(user.encode('utf-8')))
            lines = lines + __lines + ['\r\n', ]
        return lines


    def email_analytics(self, reports_by_user, timestamp):
        fmt_addr = lambda d: '"{name}"<{email}>'.format(**d)
        host = self.sender_details['server']
        port = self.sender_details['port']
        password = self.sender_details['password']
        user = self.sender_details['email']
        sender = fmt_addr(self.sender_details)
        lines = self.format_text_content(reports_by_user)
        s = SendEmail(host, port=port, user=user, password=password)
        ts = u'=== Report Timestamp: %s ===\n\n'%(timestamp.encode('utf-8'))
        body = ts + u"\n".join(lines)
        subject = self.sender_details['subject']
        all_sent = []
        recipients = [fmt_addr(rp)  for rp in self.recipients]
        r = s.send_mime_message(sender, cc=recipients, subject=subject,
                            body_content_type='plain', body=body)
        return r

