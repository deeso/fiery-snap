from fiery_snap.impl.util.page import Page, AddHandlesPage, \
               ListHandlesPage, RemoveHandlesPage, ConsumePage, \
               JsonUploadPage, TestPage

from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message
from fiery_snap.impl.util.mongo_client_impl import MongoClientImpl

from simple_pastebin_client.api import PasteBinApiClient
from datetime import datetime
import json
import logging
import traceback

TS_FMT = "%Y-%m-%d %H:%M:%S"
TIME_PS_STARTED = datetime.now().strftime(TS_FMT)

def unix_timestamp_to_format(unix_ts):
    if unix_ts is None:
        return datetime.utcnow().strftime(TS_FMT)
    if isinstance(unix_ts, str):
        unix_ts = int(unix_ts)
    dt = datetime.fromtimestamp(unix_ts)
    return dt.strftime(TS_FMT)



class PastebinClientImpl(object):
    REQUIRED_CONFIG_PARAMS = ['handle', 'name']
    OPTIONAL_CONFIG_PARAMS = [['sleep_time', 60],
                              ['last_ts', None],
                              ['last_paste_key', None],
                              ['content', 'data'],
                              ['limit', 10],
                              ['unix', 0], 
                              ]

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

            if self.unix is None or \
               long(p['unix']) > long(self.unix):
                self.unix = p['unix']
                self.last_ts = unix_timestamp_to_format(p['unix'])
                self.last_paste_key = p['paste_key']

            js['meta'] = p
            js['p_id'] = p['paste_key']
            js['link'] = p['paste']
            js['timestamp'] = p['timestamp']
            n = datetime.utcnow().strftime(TS_FMT)
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
    PASTEBIN_SOURCE_MONGODB_NAME = 'pastebin-source-mongo'
    PASTEBIN_SOURCE_MONGODB_DB = 'pastebin-source'
    PASTEBIN_SOURCE_MONGODB_COL_HANDLES = 'handles'

    REQUIRED_CONFIG_PARAMS = ['name', 'handles', 'subscribers']
    OPTIONAL_CONFIG_PARAMS = [['last_tss', {}],
                              ['listening_port', 20209],
                              ['listening_address', ''],
                              ['sleep_time', 30.0],
                              ['dbname', PASTEBIN_SOURCE_MONGODB_DB],
                              ['colname', PASTEBIN_SOURCE_MONGODB_COL_HANDLES],
                              ['mongo_name', PASTEBIN_SOURCE_MONGODB_NAME],
                              ['mongo_uri', None],
                              ['mongo_host', None],
                              ['mongo_port', 27017],
                              ['update_from_mongo', True],

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
                last_tss[h] = {'handle':h, 'timestamp':None, 'paste_key': None}
        
        self.config['last_tss'] = last_tss
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
            ts_handle_infos = self.config['last_tss']
            for h, v in handle_infos.items():
                if h in ts_handle_infos:
                    ts_handle_infos[h].update(v)
                elif self.update_from_mongo:
                    ts_handle_infos[h] = v
            self.config['last_tss'] = ts_handle_infos
            self.update_mongo_handles()

    def new_mongo_handle_client(self):
        client_params = {}
        client_params['id_keys'] = ['handle',]
        client_params['dbname'] = self.dbname
        client_params['colname'] = self.colname
        client_params['uri'] = self.mongo_uri
        client_params['name'] = self.mongo_name
        s = '{name}: {uri} {dbname}[{colname}]'.format(**client_params)
        logging.debug("Initializing MongoClient to: %s"%(s))
        return MongoClientImpl(**client_params)

    def update_mongo_handles(self):
        ts_handle_infos = self.config['last_tss']
        self.new_mongo_handle_client().inserts(ts_handle_infos.values(),
                                               dbname=self.dbname,
                                               colname=self.colname,
                                               update=True,)
        if self.update_from_mongo:
            handle_infos = self.read_mongo_handles()
            for h, v in handle_infos.items():
                if h not in ts_handle_infos:
                    ts_handle_infos[h] = v
            self.config['last_tss'] = ts_handle_infos

    def save_mongo_handle(self, handle):
        ts_info = self.config['last_tss'].get(handle)
        if ts_info is None:
            return
        i = self.new_mongo_handle_client().get_one(dbname=self.dbname, 
                                                   colname=self.colname, 
                                                   obj_dict={'handle': handle})
        i.update(ts_info)
        r = self.new_mongo_handle_client().inserts([i, ],
                                               dbname=self.dbname,
                                               colname=self.colname,
                                               update=True,)
        return r

    def read_mongo_handles(self):
        handle_recs = self.new_mongo_handle_client().get_all(dbname=self.dbname, 
                                                             colname=self.colname, 
                                                             obj_dict={})
        handle_infos = {}
        for h in handle_recs:
            k = h.get('handle')
            v = {'timestamp':h.get('timestamp', None), 
                 'paste_key':h.get('paste_key', None), 'handle':k}
            if v['timestamp'] == None:
                v['timestamp'] = TIME_PS_STARTED
            handle_infos[k] = v
        return handle_infos

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
        global TIME_PS_STARTED
        last_tss = self.config['last_tss']
        return last_tss[handle] if handle in last_tss else TIME_PS_STARTED

    def set_last_ts(self, handle, last_id, last_ts):
        ts_handle_infos = self.config['last_tss']
        if last_ts is None:
            last_ts = datetime.utcnow().strftime(TS_FMT)
        if last_id is not None:
            ts_handle_infos[handle]['paste_key'] = last_id
            ts_handle_infos[handle]['timestamp'] = last_ts
            if self.use_mongo:
                self.save_mongo_handle(handle)

    def get_last_ts(self, handle):
        global TIME_PS_STARTED
        ts_handle_infos = self.config['last_tss']
        return ts_handle_infos[handle]['timestamp'] if handle in ts_handle_infos else TIME_PS_STARTED


    def consume(self):
        all_pastes = {}

        for handle in self.config.get('handles', []):
            last_ts = self.get_last_ts(handle)
            # TODO convert to event based consumption
            tc = self.new_client(handle, last_ts)
            logging.debug("Consuming posts from: %s" % handle)
            posts = tc.consume()
            all_pastes[handle] = posts
            if tc.last_paste_key is not None:
                self.set_last_ts(handle, tc.last_paste_key, tc.last_ts)

        return all_pastes

    def consume_publish_lockstep(self):
        all_pastes = {}

        ts_handle_infos = self.config['last_tss']
        for handle in ts_handle_infos:
            last_ts = self.get_last_ts(handle)
            # TODO convert to event based consumption
            tc = self.new_client(handle, last_ts)
            logging.debug("Consuming posts from: %s" % handle)
            posts = tc.consume()
            all_pastes[handle] = posts
            if tc.last_paste_key is not None:
                self.set_last_ts(handle, tc.last_paste_key, tc.last_ts)
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
