from fiery_snap.impl.util.page import Page, AddHandlesPage, \
               ListHandlesPage, RemoveHandlesPage, ConsumePage, \
               JsonUploadPage, TestPage, MongoSearchPage

from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message
from fiery_snap.utils import parsedate_to_datetime
from fiery_snap.impl.util.mongo_client_impl import MongoClientImpl
import threading
import web
import json
from pymongo import MongoClient
import logging
import sys
import time

class MongoStore(IOBase):
    KEY = 'simple-mongo-store'
    DEFAULT_DB = 'default_db'
    DEFAULT_COLLECTION = 'default_col'
    REQUIRED_CONFIG_PARAMS = ['name', 'uri', ]
    OPTIONAL_CONFIG_PARAMS = [['publishers', {}],
                              ['subscribers', {}],
                              ['data_hash_keys', None],
                              ['gen_msg_id', None],
                              ['create_id', False],
                              ['id_keys', ['msg_id', ]],
                              ['dbname', DEFAULT_DB],
                              ['colname', DEFAULT_COLLECTION],
                              ['sleep_time', 30.0],
                              ]

    def __init__(self, config_dict, add_local=False):
        pages = [TestPage, JsonUploadPage,
                 MongoSearchPage, ConsumePage]
        super(MongoStore, self).__init__(config_dict, pages=pages)
        self.output_queue = []
        rs = self.random_name(prepend='mongo-output')
        self.name = self.config.get('name', rs)
        self.uri = self.config.get('uri')
        self.db_conn = self.new_client()

    def reset(self, dbname=None, colname=None):
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        logging.debug("resetting %s[%s]" % (dbname,colname))
        return self.db_conn.reset(dbname=dbname, colname=colname)

    def reset_all(self, dbname=None, colname=None):
        super(MongoStore, self).reset_all()
        self.reset()
        return True

    def consume(self, cnt=1):
        cnt = self.message_count if hasattr(self, 'message_count') else cnt
        messages = []
        #  conn == ..io.connection.Connection
        for name, conn in self.publishers.items():
            pos = 0
            logging.debug("Attempting to consume from: type(conn)=%s" % type(conn))
            msgs = conn.consume(cnt=cnt)
            if msgs is None or len(msgs) == 0:
                continue

            # logging.debug("Retrieved %d messages from the %s queue" % (len(msgs), name))
            logging.debug("Adding messages to the internal queue" )
            for m in msgs:
                if not self.add_incoming_message(m):
                    logging.debug("Failed to add message to queue" )
        return msgs

    def consume_and_publish(self):
        self.consume(-1)
        all_msgs = self.pop_all_in_messages()
        self.publish_all_messages(all_msgs)
        return all_msgs

    def publish_all_messages(self, all_msgs, update=True):
        cnt = 0
        dbname = self.config['dbname']
        colname = self.config['colname']
        json_msgs = [i.as_json() for i in all_msgs]
        self.db_conn.inserts(json_msgs, dbname, colname, update=update)
        logging.debug("Published %d msgs to %s[%s]" % (len(json_msgs), dbname, colname))

    def new_client(self):
        return MongoClientImpl(**self.config)

    def is_empty(self, dbname=None, colname=None):
        results = super(MongoStore, self).is_empty()
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        is_empty = self.db_conn.is_empty(dbname=dbname, colname=colname)
        results.update({'%s[%s]'%(dbname, colname): is_empty})
        return results

    def is_db_empty(self, dbname=None, colname=None):
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        is_empty = self.db_conn.is_empty(dbname=dbname, colname=colname)
        return {'%s[%s]'%(dbname, colname): is_empty}

    def handle(self, path, data):
        logging.debug("handling request (%s)" % (path))
        if path == TestPage.NAME:
            return {'msg': 'success'}
        if path == ConsumePage.NAME:
            return_posts = 'return_posts' in data
            msg_posts = self.consume_and_publish()
            all_posts = [i.toJSON() for i in msg_posts]
            num_posts = len(all_posts)
            r = {'msg': 'Consumed %d posts'%num_posts, 'all_posts':None}
            if return_posts:
                r['all_posts']= all_posts
            return r
            return {'msg': 'completed a consume cycle'}
        elif path == MongoSearchPage.NAME:
            query = data.get('query', {})
            dbname = data.get('dbname', self.config['dbname'])
            colname = data.get('colname', self.config['colname'])
            results = self.db_conn.get_all(dbname=dbname, colname=colname, obj_dict=query)
            if results is None:
                return {}

            for r in results:
                if '_id' in r:
                    r['_id'] = str(r['_id'])
            return results

        elif path == JsonUploadPage.NAME:
            update = data.get('update', True)
            direct = data.get('direct', False)
            dbname = data.get('dbname', self.config['dbname'])
            colname = data.get('colname', self.config['colname'])
            entries = data.get('entries', [])
            entry = data.get('entry', None)
            if entry is not None and len(entry) > 0:
                entries.append(entry)

            if not direct:
                messages = []
                for e in entries:
                    m = Message(e)
                    self.add_incoming_message(m)
                dbname = self.config['dbname']
                colname = self.config['colname']
                return {'msg': 'queued %d messages for publication to %s[%s]' % (len(entries), dbname, colname)}
            else:
                results = self.db_conn.inserts(entries, dbname, colname, update=update)
                return {'msg': 'Consumed messages and put them in %s[%s]' % (dbname, colname)}

        return {'error': 'unable to handle message type: %s' % path}
