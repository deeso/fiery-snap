from fiery_snap.impl.util.page import Page, AddHandlesPage, \
               ListHandlesPage, RemoveHandlesPage, ConsumePage, \
               JsonUploadPage, TestPage

from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message
from fiery_snap.utils import parsedate_to_datetime


from datetime import datetime
import json
from twitter import Api
import logging
import traceback

TS_FMT = "%Y-%m-%d %H:%M:%S"
TIME_NOW = datetime.now().strftime(TS_FMT)


class TwitterClientImpl(object):
    TWT_FMT = 'https://twitter.com/{}/status/{}'
    URL_LOC = 'https://t.co'
    REQUIRED_CONFIG_PARAMS = ['consumer_key', 'consumer_secret',
                              'access_token', 'access_token_secret',
                              'handle', 'name']
    OPTIONAL_CONFIG_PARAMS = [['sleep_on_rate_limit', True],
                              ['last_id', None],
                              ['ignore_rts', True],
                              ['content', 'text'],
                              ['limit', 10], ]

    def __init__(self, **kargs):
        self.api = None
        self.last_id = None
        self.ignore_rts = True

        for k in self.REQUIRED_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k))

        for k, v in self.OPTIONAL_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k, v))

        # print "lastid= %s" % str(self.last_id)

    def consume(self):

        _handle = self.handle if self.handle[0] == '@' else '@'+self.handle
        msgs = []
        tweets = []
        keep_going = False
        m = 'Failed to retrieve posts with the following exception:\n{}'
        try:
            keep_going = self.test_source()
        except:
            logging.debug(m.format(traceback.format_exc()))
            try:
                if self.api is not None:
                    self.api = None
                    keep_going = self.test_source()
            except:
                logging.debug(m.format(traceback.format_exc()))

        if not keep_going:
            return msgs

        try:
            tweets = self.api.GetUserTimeline(screen_name=_handle,
                                              count=self.limit,
                                              since_id=self.last_id)
        except:
            m = 'Failed to retrieve posts with the following exception:\n{}'
            logging.debug(m.format(traceback.format_exc()))

        for p in tweets:
            js = {}
            if p.text.startswith('RT') and self.ignore_rts:
                # Ignore retweets
                continue

            if self.last_id is None or \
               long(p.id) > long(self.last_id):
                self.last_id = str(p.id)

            js['meta'] = p.AsDict()
            js['tm_id'] = js['meta']['id']

            js['references'] = [self.TWT_FMT.format(self.handle, p.id), ]
            js['link'] = self.TWT_FMT.format(self.handle, p.id)
            js['timestamp'] = parsedate_to_datetime(p.created_at)
            n = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            js['obtained_timestamp'] = n
            if self.content is None or self.content not in js['meta']:
                js['content'] = json.dumps(p.AsDict())
                js['forced_content'] = True
                # \/ for internal debugging (this does not JSON dump)
                # js['raw'] = p
            else:
                js['content'] = getattr(p, self.content)
                # \/ for internal debugging (this does not JSON dump)
                js['forced_content'] = False

            js['source'] = self.name
            js['source_type'] = 'twitter'
            js['user'] = self.handle.strip('@')
            msgs.append(Message(js))
        return msgs

    def test_source(self):
        if self.api is None:
            w = self.sleep_on_rate_limit
            self.api = Api(self.consumer_key,
                           self.consumer_secret,
                           self.access_token,
                           self.access_token_secret,
                           sleep_on_rate_limit=w)
        if self.api is None:
            raise Exception("Unable to connect to twitter")
        return self.api.VerifyCredentials()


class TwitterSource(IOBase):
    KEY = 'simple-in-twitter'
    REQUIRED_CONFIG_PARAMS = ['name', 'consumer_key', 'consumer_secret',
                              'access_token', 'access_token_secret',
                              'handles', 'subscribers']
    OPTIONAL_CONFIG_PARAMS = [['sleep_on_rate_limit', True],
                              ['last_ids', {}],
                              ['listening_port', 20202],
                              ['listening_address', ''],
                              ['sleep_time', 30.0],
                              ]

    def __init__(self, config_dict, add_local=False):
        pages = [JsonUploadPage, TestPage,
                 Page, AddHandlesPage, RemoveHandlesPage,
                 ConsumePage]
        IOBase.__init__(self, config_dict, pages=pages)
        self.output_queue = []
        rs = self.random_name(prepend='twitter-input')
        self.name = self.config.get('name', rs)
        last_ids = self.config.get('last_ids', {})
        for h in self.config.get('handles', []):
            if h not in last_ids:
                last_ids[h] = None

        self.config['last_ids'] = last_ids

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

    def get_last_id(self, handle):
        global TIME_NOW
        last_ids = self.config['last_ids']
        return last_ids[handle] if handle in last_ids else TIME_NOW

    def set_last_id(self, handle, last_id):
        last_ids = self.config['last_ids']
        last_ids[handle] = last_id

    def consume(self):
        all_posts = {}

        for handle in self.config.get('handles', []):
            last_id = self.get_last_id(handle)
            # TODO convert to event based consumption
            tc = self.new_client(handle, last_id)
            logging.debug("Consuming posts from: %s" % handle)
            posts = tc.consume()
            all_posts[handle] = posts
            self.set_last_id(handle, tc.last_id)

        return all_posts

    def consume_publish_lockstep(self):
        all_posts = {}

        for handle in self.config.get('handles', []):
            last_id = self.get_last_id(handle)
            # TODO convert to event based consumption
            tc = self.new_client(handle, last_id)
            logging.debug("Consuming posts from: %s" % handle)
            posts = tc.consume()
            all_posts[handle] = posts
            self.set_last_id(handle, tc.last_id)
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
        for handle, posts in all_posts.items():
            m = "Publishing msgs %d msgs from %s"
            logging.debug(m % (len(posts), handle))
            for msg in posts:
                msgs.append(msg)

        self.publish_all_msgs(msgs)
        logging.debug("Published %d msgs to all subscribers" % (len(msgs)))
        return msgs

    def new_client(self, handle, last_id):
        return TwitterClientImpl(handle=handle, last_id=last_id, **self.config)
