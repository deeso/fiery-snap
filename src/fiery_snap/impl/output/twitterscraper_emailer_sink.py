from fiery_snap.impl.util.page import ConsumePage, \
               JsonUploadPage, TestPage, EmailInfoPage, MongoSearchPage

from fiery_snap.io.io_base import IOBase
from fiery_snap.impl.util.simplified_email import SendEmail
from fiery_snap.impl.util.mongo_client_impl import MongoClientImpl
from ioc_regex import consts
import logging
from datetime import datetime

TS_FMT = "%Y-%m-%d %H:%M:%S"
TIME_NOW = datetime.now().strftime(TS_FMT)

TWTR_URL = u"https://twitter.com/i/web/status/"
CONTENT_ARTIFACTS = 'content_artifacts'

REPORT_ITEMS = [consts.HASHES, consts.DOMAIN, consts.EMAIL, consts.URL, consts.URL, consts.IP]


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
                              # send reports out every X minutes
                              ['start_timestamp', None],
                              # send reports out every X minutes
                              ['sleep_time', 5.0*60],
                              ['recipients', []],
                              ['sender_details', None], ]

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

    def init_state(self, timestamp=None):
        m = "Initializing state with timestamp: %s"
        logging.debug(m % (str(timestamp)))
        m = "Initializing Mongo State with %s[%s]['timestamp'] = : %s"
        t = (self.state_dbname, self.state_colname, str(timestamp))
        logging.debug(m % t)
        state = self.state_db_conn.get_one(dbname=self.state_dbname,
                                           colname=self.state_colname,
                                           obj_dict={})
        if state is None:
            # do other stuff here
            state = {'obtained_timestamp': timestamp}
            self.state_db_conn.inserts([state, ],
                                       dbname=self.state_dbname,
                                       colname=self.state_colname)
        elif 'obtained_timestamp' not in state:
            state['obtained_timestamp'] = timestamp
            self.state_db_conn.inserts([state, ],
                                       dbname=self.state_dbname,
                                       colname=self.state_colname,
                                       update=True,)
        return state

    def reset_all(self, dbname=None, colname=None):
        self.reset(dbname=dbname, colname=colname)
        return True

    def reset(self, dbname=None, colname=None):
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        logging.debug("resetting state %s[%s]" % (dbname, colname))
        return self.state_db_conn.reset(dbname=dbname, colname=colname)

    def get_last_consumed_timestamp(self):
        state = self.state_db_conn.get_one(dbname=self.state_dbname,
                                           colname=self.state_colname,
                                           obj_dict={})
        if state is not None:
            return state.get('obtained_timestamp', None)
        return None

    def set_last_consumed_timestamp(self, timestamp):
        m = "Setting timestamp from last report result: %s"
        logging.debug(m % timestamp)
        state = self.state_db_conn.get_one(dbname=self.state_dbname,
                                           colname=self.state_colname,
                                           obj_dict={})
        if state is None:
            m = "Empty state? initializing it with current timestamp"
            logging.debug(m)
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
        logging.debug("Last time analytics were pulled: %s" % timestamp)
        results = self.pull_analytics(timestamp)
        m = "Pulled the analytics and found %d records to report about"
        logging.debug(m % (len(results)))

        timestamps = [v['obtained_timestamp'] for v in results]
        timestamps = sorted(timestamps)
        last_timestamp = timestamps[-1] if len(timestamps) > 0 else None
        if last_timestamp is None or len(results) == 0:
            logging.debug("Nothing to report")
            return {'result': {},
                    'reports_by_user': {},
                    'timestamp': timestamp,
                    }
        logging.debug("First timestamp in this batch: %s" % (timestamps[0]))
        logging.debug("Last timestamp in this batch: %s" % (last_timestamp))
        logging.debug("Size of this batch: %s" % (len(timestamps)))
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
        client_params['id_keys'] = ['_id', ]
        client_params['dbname'] = self.mongo_state_config['dbname']
        client_params['colname'] = self.mongo_state_config['colname']
        client_params['uri'] = self.mongo_state_config['uri']
        client_params['name'] = self.mongo_state_config['name']
        s = '{name}: {uri} {dbname}[{colname}]'.format(**client_params)
        logging.debug("Initializing MongoClient to: %s" % (s))
        return MongoClientImpl(**client_params)

    def new_result_client(self):
        client_params = {}
        client_params['dbname'] = self.mongo_result_config['dbname']
        client_params['colname'] = self.mongo_result_config['colname']
        client_params['uri'] = self.mongo_result_config['uri']
        client_params['name'] = self.mongo_result_config['name']
        s = '{name}: {uri} {dbname}[{colname}]'.format(**client_params)
        logging.debug("Initializing MongoClient to: %s" % (s))
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
                t = (path, command)
                results = {'msg': 'Not a valid command: %s["%s"]' % t}
            return results
        elif path == JsonUploadPage.NAME:
            return {}
        return {'error': 'unable to handle message type: %s' % path}

    @classmethod
    def parse(cls, config_dict):
        global TIME_NOW
        new_config_dict = {}
        new_config_dict['name'] = config_dict.get('name')
        v = config_dict.get('start_timestamp', TIME_NOW)
        new_config_dict['start_timestamp'] = v
        new_config_dict['service'] = config_dict.get('service')

        mrc = config_dict.get('mongo-result-config', None)
        if mrc is not None:
            new_config_dict['mongo_result_config'] = mrc

        msc = config_dict.get('mongo-state-config', None)
        if msc is not None:
            new_config_dict['mongo_state_config'] = msc

        new_config_dict['publishers'] = config_dict.get('publishers', {})
        new_config_dict['subscribers'] = config_dict.get('subscribers', {})
        new_config_dict['dbname'] = config_dict.get('dbname', cls.DEFAULT_DB)
        v = config_dict.get('colname', cls.DEFAULT_COLLECTION)
        new_config_dict['colname'] = v
        new_config_dict['sleep_time'] = config_dict.get('sleep_time', 30.0)
        new_config_dict['recipients'] = []
        for r in config_dict.get('recipients', {}).values():
            # name, email
            if isinstance(r, list) and len(r) == 2:
                d = {'name': r[0], 'email': r[1]}
                new_config_dict['recipients'].append(d)
            elif isinstance(r, dict) and 'name' in r and 'email' in r:
                new_config_dict['recipients'].append(r)
            elif isinstance(r, list):
                raise Exception("Expecting list containing ['name', 'email']")
            elif isinstance(r, dict):
                e = "Expecting list containing {'name':name, 'email':email}"
                raise Exception(e)
            else:
                e = "Expecting list or dict: ['name', 'email']" +\
                    " or {'name':name, 'email':email}"
                raise Exception(e)

        if len(new_config_dict['recipients']) == 0:
            raise Exception("Email recipients must be specified")

        sender_details = {}
        sender_info = config_dict.get('sender-details', {})
        sender_details['name'] = sender_info.get('name', None)
        sender_details['email'] = sender_info.get('email', None)
        sender_details['password'] = sender_info.get('password', None)
        v = sender_info.get('subject', cls.EMAILER_SUBJECT)
        sender_details['subject'] = v
        sender_details['server'] = sender_info.get('server', None)
        sender_details['port'] = sender_info.get('port', None)
        sender_details['tls'] = sender_info.get('tls', True)

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
        reports = {}
        reports_by_user = {}
        for r in results:
            tm_id = r['tm_id']
            entities = r['entities']
            tags = r['tags']
            htags = entities[consts.HASH_TAG]
            keywords = entities[consts.KEYWORDS]

            reports = self.update_with_defanged_entities_content(r, tm_id, reports)
            user = reports[tm_id]['user']
            if user not in reports_by_user:
                reports_by_user[user] = {}
            n_rec = {consts.KEYWORDS: keywords,
                     consts.HASH_TAG: htags,
                     'tags': tags, }
            reports_by_user[user][tm_id] = n_rec
            for k, v in reports[tm_id].items():
                if k == 'user':
                    continue
                n_rec[k] = v
        return reports_by_user

    def extract_linked_content(self, record, tm_id, reports):
        linked_content = record['linked_content']
        _df_domains = linked_content[consts.DF_DOMAIN]
        _df_ips = linked_content[consts.DF_IP]
        _df_urls = linked_content[consts.DF_URL]
        domains = reports[tm_id][consts.DF_DOMAIN] + _df_domains
        reports[tm_id][consts.DF_DOMAIN] = domains
        reports[tm_id][consts.DF_URL] = reports[tm_id][consts.DF_URL] + _df_urls
        reports[tm_id][consts.DF_IP] = reports[tm_id][consts.DF_IP] + _df_ips
        return reports

    def extract_entity_item(self, item_name, record, tm_id, reports):
        r = record
        e2 = r['entities']
        items = e2.get(item_name, [])
        items = sorted(set(items))
        reports[tm_id][item_name] = items
        return reports

    def update_with_defanged_entities_content(self, record, tm_id, reports):
        r = record
        items = [consts.HASHES, consts.DOMAIN,
                 consts.EMAIL, consts.URL, consts.IP,
                 consts.DF_DOMAIN,
                 consts.DF_EMAIL, consts.DF_URL, consts.DF_IP]
        reports[tm_id] = dict([(i, {}) for i in items])
        reports[tm_id]['content'] = r['content']
        reports[tm_id]['user'] = r['user']
        reports[tm_id]['obtained_timestamp'] = r.get('obtained_timestamp', '')
        reports[tm_id]['timestamp'] = r.get('timestamp', '')

        for item_name in items:
            reports = self.extract_entity_item(item_name,
                                               record,
                                               tm_id,
                                               reports)
        reports = self.extract_linked_content(record, tm_id, reports)
        reports = self.filter_twitter_urls(record, tm_id, reports)
        return reports

    def filter_twitter_urls(self, record, tm_id, reports):
        v = [i for i in reports[tm_id][consts.URL] if i.strip().find('://t.co/')]
        # v = [i for i in v if i.strip().find('https://t.co/') != 0]
        # v = [i for i in v if i.strip().find('http://t.co/') != 0]
        reports[tm_id][consts.URL] = sorted(v)
        return reports

    def defang(self, list_content):
        nl = u''
        for i in list_content:
            t = i
            try:
                i = i.replace(u'.', u'[.]')
                t = i.encode('utf-8')
            except:
                logging.debug("Failed to encode string URL to utf-8")
                continue

            if len(nl) > 0:
                try:
                    nl = nl + u', ' + t.encode('utf-8')
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
        twtr_url = lambda tm_id: TWTR_URL + str(tm_id).encode('utf-8')
        ts_ec = lambda ts: ts.encode('utf-8')
        startheader = u'==== Start {} Section ===='
        endheader = u'==== End {} Section ===='
        tweet_header = u'|===     [{}] {}'
        for user, user_recs in reports_by_user.items():
            __lines = []
            __lines.append(startheader.format(user.encode('utf-8')))
            for tm_id, r in user_recs.items():
                add_tags_kws = False
                timestamp = r['timestamp']
                # t = ()
                logging.info("%s: %s" % (ts_ec(timestamp), twtr_url(tm_id)))
                _lines = [tweet_header.format(ts_ec(timestamp), twtr_url(tm_id))]
                if len(r[consts.HASHES]) > 0:
                    m = u"|========     hashes: {}"
                    _lines.append(m.format(self.defang(r[consts.HASHES])))
                    add_tags_kws = True
                if len(r[consts.DF_DOMAIN]) > 0:
                    m = u"|========     defanged domains: {}"
                    _lines.append(m.format(self.defang(r[consts.DF_DOMAIN])))
                    add_tags_kws = True
                if len(r[consts.DF_URL]) > 0:
                    m = u"|========     defanged urls: {}"
                    _lines.append(m.format(self.defang(r[consts.DF_URL])))
                    add_tags_kws = True
                if len(r[consts.DF_IP]) > 0:
                    m = u"|========     defanged ips: {}"
                    _lines.append(m.format(self.defang(r[consts.DF_IP])))
                    add_tags_kws = True
                if len(r[consts.DF_EMAIL]) > 0:
                    m = u"|========     defanged emails: {}"
                    _lines.append(m.format(self.defang(r[consts.DF_EMAIL])))
                    add_tags_kws = True
                if len(r[consts.HASH_TAG]) > 0 and add_tags_kws:
                    m = u"|========     tags: {}"
                    _lines.append(m.format(self.defang(r[consts.HASH_TAG])))
                if len(r[consts.KEYWORDS]) > 0 and add_tags_kws:
                    m = u"|========     keywords: {}"
                    _lines.append(m.format(self.defang(r[consts.KEYWORDS])))

                if len(_lines) > 1:
                    __lines = __lines + _lines + ['|', ]
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
        ts = u'=== Report Timestamp: %s ===\n\n' % (timestamp.encode('utf-8'))
        body = ts + u"\n".join(lines)
        subject = self.sender_details['subject']
        recipients = [fmt_addr(rp) for rp in self.recipients]
        r = s.send_mime_message(sender, cc=recipients, subject=subject,
                                body_content_type='plain', body=body)
        return r
