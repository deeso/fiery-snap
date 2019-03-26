from fiery_snap.impl.util.page import Page, AddHandlesPage, \
    ListHandlesPage, RemoveHandlesPage, ConsumePage, \
    JsonUploadPage, TestPage

from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message
from fiery_snap.utils import parsedate_to_datetime
from fiery_snap.impl.util.mongo_client_impl import MongoClientImpl
from fiery_snap.utils import datetime_to_utc

from datetime import datetime
import json

import twint
import twint.output
import logging
import traceback
from twint.run import Twint
import asyncio

TS_FMT = "%Y-%m-%d %H:%M:%S"
TIME_TS_STARTED = datetime.now().strftime(TS_FMT)


def run(config, callback=None):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(Twint(config).main(callback))

class TwintClientImpl(object):
    TWT_FMT = 'https://twitter.com/{}/status/{}'
    URL_LOC = 'https://t.co'
    REQUIRED_CONFIG_PARAMS = ['handle', 'name']
    OPTIONAL_CONFIG_PARAMS = [['last_id', None],
                              ['ignore_rts', True],
                              ['content', 'text'],
                              ['limit', 10],
                              ]

    def __init__(self, **kargs):
        self.last_id = None
        self.last_ts = None
        self.ignore_rts = True

        for k in self.REQUIRED_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k))

        for k, v in self.OPTIONAL_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k, v))

        # print "lastid= %s" % str(self.last_id)

    def consume(self):

        _handle = self.handle if self.handle[0] == '@' else '@' + self.handle
        c = twint.Config()
        c.Username = _handle
        c.Store_object = True
        c.Limit = 20
        c.Profile_full = True

        build_ts = lambda to: " ".join([getattr(to, name, '') for name in ['datestamp', 'timestamp', 'timezone']])

        # TODO determine date and time of last read

        # TODO read the tweets from the profile

        # TODO filter tweets greater than the last read

        # TODO read those tweets and metadata

        # TODO convert the metadata to the expected metadata

        # TODO update last read time update last read time


        msgs = []
        tweets = []
        keep_going = False
        m = 'Failed to retrieve posts with the following exception:\n{}'
        try:
            keep_going = self.test_source()
        except:
            logging.debug(m.format(traceback.format_exc()))

        if not keep_going:
            return msgs

        try:
            # twint.run.Profile(c)
            run(c, None)
            tweets = sorted([i for i in twint.output.tweets_object if i.id > self.last_id], key=lambda to: to.id)
            twint.output.tweets_object = []
        except:
            m = 'Failed to retrieve posts with the following exception:\n{}'
            logging.debug(m.format(traceback.format_exc()))

        last_id = self.last_ts
        last_ts = self.last_id

        for p in tweets:
            js = {}
            if p.tweet.startswith('RT') and self.ignore_rts:
                # Ignore retweets
                continue

            if self.last_id is None or \
                    int(p.id) > int(self.last_id):
                last_id = int(p.id)
                last_ts = datetime_to_utc(datetimestamp=build_ts(p)).strftime(TS_FMT)

            js['meta'] = p.__dict__
            js['tm_id'] = last_id

            js['references'] = [] # [self.TWT_FMT.format(self.handle, p.id), ]
            js['link'] = p.link
            js['timestamp'] = last_ts
            n = datetime.utcnow().strftime(TS_FMT)
            js['obtained_timestamp'] = n
            # if self.content is None or self.content not in js['meta']:
            #     js['content'] = json.dumps(p.AsDict())
            #     js['forced_content'] = True
            #     # \/ for internal debugging (this does not JSON dump)
            #     # js['raw'] = p
            # else:
            #     js['content'] = getattr(p, self.content)
            #     # \/ for internal debugging (this does not JSON dump)
            #     js['forced_content'] = False
            js['forced_content'] = False
            js['content'] = p.tweet

            js['source'] = self.name
            js['source_type'] = 'twitter'
            js['user'] = c.Username
            msgs.append(Message(js))

        self.last_ts = last_ts
        self.last_id = last_id

        return msgs

    def test_source(self):
        return True


class TwintSource(IOBase):
    TWITTER_SOURCE_MONGODB_NAME = 'twitter-source-mongo'
    TWITTER_SOURCE_MONGODB_DB = 'twitter-source'
    TWITTER_SOURCE_MONGODB_COL_HANDLES = 'handles'

    KEY = 'twint-in-twitter'
    REQUIRED_CONFIG_PARAMS = ['name', 'handles', 'subscribers']
    OPTIONAL_CONFIG_PARAMS = [['sleep_on_rate_limit', True],
                              ['ts_handle_infos', {}],
                              ['listening_port', 20202],
                              ['listening_address', ''],
                              ['sleep_time', 30.0],
                              ['dbname', TWITTER_SOURCE_MONGODB_DB],
                              ['colname', TWITTER_SOURCE_MONGODB_COL_HANDLES],
                              ['mongo_name', TWITTER_SOURCE_MONGODB_NAME],
                              ['mongo_uri', None],
                              ['mongo_host', None],
                              ['mongo_port', 27017],
                              ['update_from_mongo', False],
                              ]

    def __init__(self, config_dict, add_local=False):
        pages = [JsonUploadPage, TestPage,
                 Page, AddHandlesPage, RemoveHandlesPage,
                 ConsumePage]
        IOBase.__init__(self, config_dict, pages=pages)
        self.output_queue = []
        rs = self.random_name(prepend='twitter-input')
        self.name = self.config.get('name', rs)
        ts_handle_infos = {}
        for h in self.config.get('handles', []):
            if h not in ts_handle_infos:
                ts_handle_infos[h] = {'handle': h, 'timestamp': None, 'tm_id': None}

        self.config['ts_handle_infos'] = ts_handle_infos
        self.dbname = self.config.get('dbname')
        self.colname = self.config.get('colname')
        self.mongo_name = self.config.get('mongo_name')
        self.mongo_uri = self.config.get('mongo_uri')
        self.mongo_host = self.config.get('mongo_host')
        self.mongo_port = self.config.get('mongo_port')
        self.update_from_mongo = self.config.get('update_from_mongo')

        self.use_mongo = self.mongo_uri is not None or self.mongo_host is not None
        if self.mongo_host is not None and self.mongo_uri is None:
            self.mongo_uri = "mongodb://%s:%s" % (self.mongo_host, self.mongo_port)

        if self.use_mongo:
            handle_infos = self.read_mongo_handles()
            ts_handle_infos = self.config['ts_handle_infos']
            for h, v in list(handle_infos.items()):
                if h in ts_handle_infos:
                    ts_handle_infos[h].update(v)
                elif self.update_from_mongo:
                    ts_handle_infos[h] = v
            self.config['ts_handle_infos'] = ts_handle_infos
            self.update_mongo_handles()

    @classmethod
    def parse(cls, block, **kargs):
        return cls(block, **kargs)

    def handle(self, path, data):
        logging.debug("handling request (%s)" % (path))

        if path == JsonUploadPage.NAME:
            messages = []
            msgs = data.get('msgs', [])
            msg = data.get('msg', None)
            if msg is not None and len(msg) > 0:
                msgs.append(msg)

            for msg in msgs:
                m = Message(msg)
                messages.append(m)

            if len(messages) > 0:
                self.publish_all_msgs(messages)
            m = 'published %d messages in the queue'
            return {'msg': m % len(messages)}

        if path == RemoveHandlesPage.NAME:
            handles = []
            if 'handle' in data:
                handles = [data.get('handle'), ]
            elif 'handles' in data:
                handles = data.get('handles')
            if len(handles) > 0:
                logging.debug("Removing handles: %s" % handles)
                return self.rm_handles(handles)
        if path == AddHandlesPage.NAME:
            handles = []
            if 'handle' in data:
                handles = [data.get('handle'), ]
            elif 'handles' in data:
                handles = data.get('handles')
            if len(handles) > 0:
                logging.debug("Adding handles: %s" % handles)
                return self.add_handles(handles)
        elif path == ListHandlesPage.NAME:
            data = {'handles': self.config.get('handles')}
            return json.dumps(data)
        elif path == "shutdown":
            self.svc.stop()
            return {'msg': 'killing it'}
        elif path == ConsumePage.NAME:
            return_posts = 'return_posts' in data
            msg_posts = self.consume_and_publish()
            all_posts = [i.toJSON() for i in msg_posts]
            num_posts = len(all_posts)
            r = {'msg': 'Consumed %d posts' % num_posts,
                 'all_posts': all_posts}
            if return_posts:
                r['all_posts'] = all_posts
            return r
        return {'error': 'unable to handle message type: %s' % path}

    def add_handle(self, handle):
        self.add_handles([handle, ])

    def add_handles(self, handles):
        _handles = set(handles)
        handles = set(self.config.get('handles', []))
        handles |= _handles
        self.config.get('handles', list(handles))
        return handles

    def rm_handles(self, handles):
        _handles = set(handles)
        handles = set(self.config.get('handles', []))
        handles = [i for i in handles if i not in _handles]
        self.config.get('handles', list(handles))
        return handles

    def remove_handle(self, handle):
        self.remove_handles([handle, ])

    def remove_handles(self, handles):
        _handles = set(handles)
        handles = set(self.config.get('handles', []))
        handles = [i for i in handles if i not in _handles]
        self.config.get('handles', handles)
        return handles

    def get_last_tm_read(self, handle):
        global TIME_TS_STARTED
        ts_handle_infos = self.config['ts_handle_infos']
        return ts_handle_infos[handle]['tm_id'] if handle in ts_handle_infos else TIME_TS_STARTED

    def set_last_tm_read(self, handle, last_id, last_ts):
        ts_handle_infos = self.config['ts_handle_infos']
        if last_ts is None:
            last_ts = datetime.utcnow().strftime(TS_FMT)
        if last_id is not None:
            ts_handle_infos[handle]['tm_id'] = last_id
            ts_handle_infos[handle]['timestamp'] = last_ts
            if self.use_mongo:
                self.save_mongo_handle(handle)

    def consume(self):
        all_posts = {}

        for handle in self.config.get('handles', []):
            last_id = self.get_last_tm_read(handle)
            # TODO convert to event based consumption
            tc = self.new_client(handle, last_id)
            logging.debug("Consuming posts from: %s" % handle)
            posts = tc.consume()
            all_posts[handle] = posts
            if tc.last_id is not None:
                self.set_last_tm_read(handle, tc.last_id, tc.last_ts)

        return all_posts

    def new_mongo_handle_client(self):
        client_params = {}
        client_params['id_keys'] = ['handle', ]
        client_params['dbname'] = self.dbname
        client_params['colname'] = self.colname
        client_params['uri'] = self.mongo_uri
        client_params['name'] = self.mongo_name
        s = '{name}: {uri} {dbname}[{colname}]'.format(**client_params)
        logging.debug("Initializing MongoClient to: %s" % (s))
        return MongoClientImpl(**client_params)

    def read_mongo_handles(self):
        handle_recs = self.new_mongo_handle_client().get_all(dbname=self.dbname,
                                                             colname=self.colname,
                                                             obj_dict={})
        handle_infos = {}
        for h in handle_recs:
            k = h.get('handle')
            v = {'timestamp': h.get('timestamp', None),
                 'tm_id': h.get('tm_id', None), 'handle': k}
            # decided to set this so that we are not
            # unnecessarily searching in the past
            if v['timestamp'] == None:
                v['timestamp'] = TIME_TS_STARTED
            handle_infos[k] = v
        return handle_infos

    def save_mongo_handle(self, handle):
        ts_info = self.config['ts_handle_infos'].get(handle)
        if ts_info is None:
            return
        i = self.new_mongo_handle_client().get_one(dbname=self.dbname,
                                                   colname=self.colname,
                                                   obj_dict={'handle': handle})
        i.update(ts_info)
        r = self.new_mongo_handle_client().inserts([i, ],
                                                   dbname=self.dbname,
                                                   colname=self.colname,
                                                   update=True, )
        return r

    def update_mongo_handles(self):
        ts_handle_infos = self.config['ts_handle_infos']
        self.new_mongo_handle_client().inserts(list(ts_handle_infos.values()),
                                               dbname=self.dbname,
                                               colname=self.colname,
                                               update=True, )
        if self.update_from_mongo:
            handle_infos = self.read_mongo_handles()
            for h, v in list(handle_infos.items()):
                if h not in ts_handle_infos:
                    ts_handle_infos[h] = v
            self.config['ts_handle_infos'] = ts_handle_infos

    def consume_publish_lockstep(self):
        all_posts = {}

        ts_handle_infos = self.config['ts_handle_infos']
        for handle in ts_handle_infos:
            last_id = self.get_last_tm_read(handle)
            # TODO convert to event based consumption
            tc = self.new_client(handle, last_id)
            logging.debug("Consuming posts from: %s" % handle)
            posts = tc.consume()
            all_posts[handle] = posts
            if tc.last_id is not None:
                self.set_last_tm_read(handle, tc.last_id, tc.last_ts)
            self.publish_all_posts({'handle': all_posts[handle]})
        return all_posts

    def consume_and_publish(self, msg_queue_only=False):
        # Preference should be to do the consume and publish in lockstep
        # since Twitter may rate limit, but we want to hold our place in
        # tweets reviewed
        # all_posts = self.consume()
        # return self.publish_all_posts(all_posts)
        return self.consume_publish_lockstep()

    def publish_all_posts(self, all_posts):
        msgs = []
        for handle, posts in list(all_posts.items()):
            m = "Publishing msgs %d msgs from %s"
            logging.debug(m % (len(posts), handle))
            for msg in posts:
                msgs.append(msg)

        self.publish_all_msgs(msgs)
        logging.debug("Published %d msgs to all subscribers" % (len(msgs)))
        return msgs

    def new_client(self, handle, last_id):
        return TwintClientImpl(handle=handle, last_id=last_id, **self.config)
