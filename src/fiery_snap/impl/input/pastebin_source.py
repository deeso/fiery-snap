from fiery_snap.impl.util.page import Page, AddHandlesPage, \
               ListHandlesPage, RemoveHandlesPage, ConsumePage, \
               JsonUploadPage, TestPage

from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message

from simple_pastebin_client.api import PasteBinApiClient
from datetime import datetime
import json
import logging
import traceback

TS_FMT = "%Y-%m-%d %H:%M:%S"
TIME_NOW = datetime.now().strftime(TS_FMT)


class PastebinClientImpl(object):
    REQUIRED_CONFIG_PARAMS = ['handle', 'name']
    OPTIONAL_CONFIG_PARAMS = [['sleep_time', 60],
                              ['last_ts', None],
                              ['content', 'data'],
                              ['limit', 10], ]

    def __init__(self, **kargs):

        for k in self.REQUIRED_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k))

        for k, v in self.OPTIONAL_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k, v))

        # print "lastid= %s" % str(self.last_ts)

    def consume(self):
        msgs = []
        pastes = []

        try:
            pastes = PasteBinApiClient.user_pastes_data(self.handle,
                                                        do_all=True,
                                                        after_ts=self.last_ts)
        except:
            e = traceback.format_exc()
            m = 'Failed to retrieve pastes with the following exception:\n{}'
            logging.debug(m.format(e))

        for p in pastes:
            js = {}

            if self.last_ts is None or \
               long(p['unix']) > long(self.last_ts):
                self.last_ts = str(p['unix'])

            js['meta'] = p
            js['p_id'] = p['paste_key']
            js['link'] = p['paste']
            js['timestamp'] = p['timestamp']
            n = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            js['obtained_timestamp'] = n
            if self.content is None or self.content not in js['meta']:
                js['content'] = json.dumps(p)
                js['forced_content'] = True
                # \/ for internal debugging (this does not JSON dump)
                # js['raw'] = p
            else:
                js['content'] = p['data']
                # \/ for internal debugging (this does not JSON dump)
                js['forced_content'] = False

            js['source'] = self.name
            js['source_type'] = 'pastebin'
            js['user'] = self.handle
            msgs.append(Message(js))
        return msgs

    def test_source(self):
        try:
            PasteBinApiClient.user_pastes_data(self.handle, do_all=False)
        except:
            m = "Unable to connect to pastebin with %s" % self.handle
            raise Exception(m)
        return True


class PastebinSource(IOBase):
    KEY = 'simple-in-pastebin'
    REQUIRED_CONFIG_PARAMS = ['name', 'handles', 'subscribers']
    OPTIONAL_CONFIG_PARAMS = [['last_tss', {}],
                              ['listening_port', 20209],
                              ['listening_address', ''],
                              ['sleep_time', 30.0],
                              ]

    def __init__(self, config_dict, add_local=False):
        pages = [JsonUploadPage, TestPage,
                 Page, AddHandlesPage, RemoveHandlesPage,
                 ConsumePage]
        IOBase.__init__(self, config_dict, pages=pages)
        self.output_queue = []
        rs = self.random_name(prepend='pastebin-input')
        self.name = self.config.get('name', rs)
        last_tss = self.config.get('last_tss', {})
        for h in self.config.get('handles', []):
            if h not in last_tss:
                last_tss[h] = None

        self.config['last_tss'] = last_tss

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
            m = 'published %d messages in the queue' % len(messages)
            return {'msg': m}

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
            all_pastes = [i.toJSON() for i in msg_posts]
            num_posts = len(all_pastes)
            r = {'msg': 'Consumed %d posts' % num_posts, 'all_pastes': None}
            if return_posts:
                r['all_pastes'] = all_pastes
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

    def get_last_ts(self, handle):
        global TIME_NOW
        last_tss = self.config['last_tss']
        return last_tss[handle] if handle in last_tss else TIME_NOW

    def set_last_ts(self, handle, last_ts):
        last_tss = self.config['last_tss']
        last_tss[handle] = last_ts

    def consume(self):
        all_pastes = {}

        for handle in self.config.get('handles', []):
            last_ts = self.get_last_ts(handle)
            # TODO convert to event based consumption
            tc = self.new_client(handle, last_ts)
            logging.debug("Consuming posts from: %s" % handle)
            posts = tc.consume()
            all_pastes[handle] = posts
            self.set_last_ts(handle, tc.last_ts)

        return all_pastes

    def consume_publish_lockstep(self):
        all_pastes = {}

        for handle in self.config.get('handles', []):
            last_ts = self.get_last_ts(handle)
            # TODO convert to event based consumption
            tc = self.new_client(handle, last_ts)
            logging.debug("Consuming posts from: %s" % handle)
            posts = tc.consume()
            all_pastes[handle] = posts
            self.set_last_ts(handle, tc.last_ts)
            self.publish_all_pastes({'handle': all_pastes[handle]})
        return all_pastes

    def consume_and_publish(self, msg_queue_only=False):
        # Preference should be to do the consume and publish in lockstep
        # since Twitter may rate limit, but we want to hold our place in
        # pastes reviewed
        # all_pastes = self.consume()
        # return self.publish_all_pastes(all_pastes)
        return self.consume_publish_lockstep()

    def publish_all_pastes(self, all_pastes):
        msgs = []
        for handle, posts in all_pastes.items():
            m = "Publishing msgs %d msgs from %s" % (len(posts), handle)
            logging.debug(m)
            for msg in posts:
                msgs.append(msg)

        self.publish_all_msgs(msgs)
        logging.debug("Published %d msgs to all subscribers" % (len(msgs)))
        return msgs

    def new_client(self, handle, last_ts):
        return PastebinClientImpl(handle=handle,
                                  last_ts=last_ts,
                                  **self.config)
