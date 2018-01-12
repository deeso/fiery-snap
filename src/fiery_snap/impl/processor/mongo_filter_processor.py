import logging
from fiery_snap.processor.base import BaseProcessor
from fiery_snap.impl.util.mongo_client_impl import MongoClientImpl
from pymongo import MongoClient
from fiery_snap.impl.util.service import GenericService
from fiery_snap.impl.util.page import JsonUploadPage, TestPage

import regex
import validators
from hashlib import sha256

DEBUG_CONTENT = []


class MongoFilterProcessor(BaseProcessor):
    DEFAULT_DB = 'default_db'
    DEFAULT_COLLECTION = 'default_col'
    REQUIRED_CONFIG_PARAMS = [ # 'name', 
                              'uri', ]
    OPTIONAL_CONFIG_PARAMS = [ # ['publishers', {}],
                              # ['subscribers', {}],
                              # ['subscriber_polling', 1.0],
                              # ['message_count', 100],
                              ['gen_id_from', lambda msg: ""],
                              ['id_key', 'msg_id'],
                              ['data_hash_key', None],
                              ['dbname', DEFAULT_DB],
                              ['colname', DEFAULT_COLLECTION],
                              ]

    KEY = 'MongoFilter'
    def __init__(self, name,  
                 subscriber_polling=1.0, message_count=100,
                 publishers={}, subscribers={}, 
                 service={},
                 **kargs):

        BaseProcessor.__init__(self, name, subscriber_polling=subscriber_polling,
                               message_count=message_count, 
                               publishers=publishers,
                               subscribers=subscribers,
                               service=service,
                               pages=[JsonUploadPage, TestPage])
        # self.uri = uri
        # self.gen_id_from = gen_id_from
        # self.dbname = dbname
        # self.colname = colname
        for k in self.REQUIRED_CONFIG_PARAMS:
            if k not in kargs:
                raise Exception(self.MISSING_KEY % k)

            setattr(self, k, kargs.get(k, None))
        
        for k,v in self.OPTIONAL_CONFIG_PARAMS:
            if k not in kargs:
                setattr(self, k, v)
            else:                
                setattr(self, k, kargs.get(k, None))

        self.db_conn = MongoClientImpl(subscribers=self.subscribers,
                                       publishers=self.publishers, 
                                       **kargs)

    def reset(self, dbname=None, colname=None):
        dbname = self.dbname if dbname is None else dbname
        colname = self.colname if colname is None else colname
        return self.db_conn.reset(dbname=dbname, colname=colname)

    def insert(self, content):
        dbname = self.dbname if dbname is None else dbname
        colname = self.colname if colname is None else colname
        return self.db_conn.insert(content, dbname=dbname, colname=colname)

    def insert_msg(self, msg):
        dbname = self.dbname if dbname is None else dbname
        colname = self.colname if colname is None else colname
        return self.db_conn.insert_msg(msg, dbname=dbname, colname=colname)

    def insert_msgs(self, msgs):
      r = []
      for m in msgs:
          r.append(self.db_conn.insert_msg(m, dbname=self.dbname, colname=self.colname))
      return r 

    def insert_unique_content(self, db, col, mongo_content):
        unique = False
        failed_check = True
        db_conn = self._conn[db]
        col = db_conn[col]
        if '_id' in mongo_content:
            failed_check = not self.has_obj(col_conn=col, obj_dict={'_id': mongo_content['_id']})

        if not failed_check:
            x = [i for i in col.find({'_id': sm['_id']}).limit(1)][0]
            return False, x['_id']
        return True, col.insert_one(sm).inserted_id

    def process_message(self, omessage):
        return self.db_conn.insert_msg(omessage, dbname=self.dbname, colname=self.colname)

    def process_task(self, body, kombu_message):
        jsonmsg = json.loads(kombu_message.payload)
        inserted, oid = self.process_message(jsonmsg)
        if inserted:
            self.publish(jsonmsg)
        kombu_message.ack()

    def get_all(self, dbname=None, colname=None, obj_dict={}):
        dbname = self.dbname if dbname is None else dbname
        colname = self.colname if colname is None else colname
        return self.db_conn.get_all(dbname=dbname, colname=colname, obj_dict=obj_dict)

    def get_one(self, dbname=None, colname=None, obj_dict={}):
        dbname = self.config.get('dbname') if dbname is None else dbname
        colname = self.config.get('colname') if colname is None else colname
        return self.db_conn.get_one(dbname=dbname, colname=colname, obj_dict=obj_dict)

    def has_obj(self, q_dict={}):
        return self.db_conn.has_obj(obj_dict=q_dict)

    @classmethod
    def parse(cls, config_dict, **kargs):
        name = config_dict.get('name', None)
        subscribers = config_dict.get('subscribers', {})
        publishers = config_dict.get('publishers', {})
        message_count = config_dict.get('message_count', 100)
        subscriber_polling = config_dict.get('subscriber_polling', 1.0)
        dbname = config_dict.get('dbname', None)
        colname = config_dict.get('colname', None)
        data_hash_key = config_dict.get('data_hash_key', None)
        id_key = config_dict.get('id_key', None)
        uri = config_dict.get('uri', None)
        sgen_id_from = config_dict.get('gen_id_from', None )
        gen_id_from = lambda msg: ""
        if sgen_id_from is not None and sgen_id_from.find('lambda') == 0:
            gen_id_from = eval(sgen_id_from)

        service = config_dict.get('service', {})

        return cls(name, uri=uri, dbname=dbname, colname=colname, gen_id_from=gen_id_from,
                   subscribers=subscribers, publishers=publishers,
                   message_count=message_count, subscriber_polling=subscriber_polling,
                   data_hash_key=data_hash_key, id_key=id_key,
                   service=service)

    def handle(self, path, data):
        logging.debug("handling request (%s): %s" % (path, data))
        if path == TestPage.NAME:
            {'msg': 'success'}
        return {'error': 'unable to handle message type: %s' % path}