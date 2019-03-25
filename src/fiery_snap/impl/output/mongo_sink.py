from fiery_snap.impl.util.page import ConsumePage, \
               JsonUploadPage, TestPage, MongoSearchPage

from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message
from fiery_snap.impl.util.mongo_client_impl import MongoClientImpl
import logging


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
        logging.debug("resetting %s[%s]" % (dbname, colname))
        return self.new_client().reset(dbname=dbname, colname=colname)

    def reset_all(self, dbname=None, colname=None):
        super(MongoStore, self).reset_all()
        self.reset()
        return True

    def consume(self, cnt=1):
        cnt = self.message_count if hasattr(self, 'message_count') else cnt
        messages = []
        #  conn == ..io.connection.Connection
        for name, conn in list(self.publishers.items()):
            _m = "Attempting to consume from: type(conn)=%s" % type(conn)
            logging.debug(_m)
            msgs = conn.consume(cnt=cnt)
            if msgs is None or len(msgs) == 0:
                continue

            logging.debug("Adding messages to the internal queue")
            for m in msgs:
                if not self.add_incoming_message(m):
                    logging.debug("Failed to add message to queue")
                    continue
                messages.append(m)
        return messages

    def consume_and_publish(self):
        self.consume(-1)
        all_msgs = self.pop_all_in_messages()
        self.publish_all_messages(all_msgs)
        return all_msgs

    def publish_all_messages(self, all_msgs, update=True):
        dbname = self.config['dbname']
        colname = self.config['colname']
        json_msgs = [i.as_json() for i in all_msgs]
        self.new_client().inserts(json_msgs, dbname, colname, update=update)
        _m = "Published %d msgs to %s[%s]" % (len(json_msgs), dbname, colname)
        logging.debug(_m)

    def new_client(self):
        return MongoClientImpl(**self.config)

    def is_empty(self, dbname=None, colname=None):
        results = super(MongoStore, self).is_empty()
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        is_empty = self.new_client().is_empty(dbname=dbname, colname=colname)
        results.update({'%s[%s]' % (dbname, colname): is_empty})
        return results

    def is_db_empty(self, dbname=None, colname=None):
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        is_empty = self.new_client().is_empty(dbname=dbname, colname=colname)
        return {'%s[%s]' % (dbname, colname): is_empty}

    def handle(self, path, data):
        nc = self.new_client()
        logging.debug("handling request (%s)" % (path))
        if path == TestPage.NAME:
            return {'msg': 'success'}
        if path == ConsumePage.NAME:
            return_posts = 'return_posts' in data
            msg_posts = self.consume_and_publish()
            all_posts = [i.toJSON() for i in msg_posts]
            num_posts = len(all_posts)
            r = {'msg': 'Consumed %d posts' % num_posts, 'all_posts': None}
            if return_posts:
                r['all_posts'] = all_posts
            return r
            return {'msg': 'completed a consume cycle'}
        elif path == MongoSearchPage.NAME:
            query = data.get('query', {})
            dbname = data.get('dbname', self.config['dbname'])
            colname = data.get('colname', self.config['colname'])
            results = nc.get_all(dbname=dbname,
                                 colname=colname,
                                 obj_dict=query)
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
                    messages.append(m)
                dbname = self.config['dbname']
                colname = self.config['colname']
                _t = (len(entries), dbname, colname)
                _m = 'queued %d messages for publication to %s[%s]' % _t
                return {'msg': _m}
            else:
                results = nc.inserts(entries, dbname, colname, update=update)
                _t = (dbname, colname)
                _m = 'Consumed messages and put them in %s[%s]' % _t
                return {'msg': _m}

        return {'error': 'unable to handle message type: %s' % path}
