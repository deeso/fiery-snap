import logging
from pymongo import MongoClient
import regex
import validators
from hashlib import sha256
import json

class MongoClientImpl(object):
    DEFAULT_DB = 'default_db'
    DEFAULT_COLLECTION = 'default_col'
    REQUIRED_CONFIG_PARAMS = ['uri',]
    DEFAULT_GEN_MSG_ID = lambda msg: None
    OPTIONAL_CONFIG_PARAMS = [['dbname', DEFAULT_DB],
                              ['colname', DEFAULT_COLLECTION],
                              ['gen_msg_id', None],
                              ['publishers', {}],
                              ['subscribers', {}],
                              ['id_keys', []],
                              ['gen_hash_key', None],
                              ['data_hash_keys', None],
                              ['create_id', False],
                              ]

    def __init__(self, **kargs):
        self.db_conn = None
        for k in self.REQUIRED_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k))

        for k, v in self.OPTIONAL_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k, v))

        self._conn = MongoClient(self.uri)

        if self.gen_hash_key is None and self.create_id:
          self.gen_hash_key = lambda c: sha256(str(c).encode('utf-8')).hexdigest()

    def io_callback(self, msg):
        dbname = msg.get('dbname', self.dbname)
        colname = msg.get('colname', self.colname)
        msg_id = None
        try:
            if self.gen_msg_id is not None:
                msg_id = msg.get('msg_id', self.gen_msg_id(msg))
        except:
            logging.error("Failed to generate message id")

        mongo_content = {}
        mongo_content.update(msg)
        if msg_id is not None:
            mongo_content['_id'] = msg_id

        inserted, msg_id = self.insert_unique_content(dbname, dbcol, mongo_content)
        return inserted, msg_id

    def reset(self, dbname=None, colname=None):
        colname = self.colname if colname is None else colname
        dbname = self.dbname if dbname is None else dbname
        logging.debug("resetting %s[%s]" % (dbname,colname))
        if dbname is None:
            return False
        if colname is None:
            try:
                self._conn.drop_database(dbname)
                logging.debug("resetting %s[%s] = success to drop db" % (dbname,colname))
                return True
            except:
                logging.debug("resetting %s[%s] = failed to drop db" % (dbname,colname))
                return False

        db_conn = self._conn[dbname]
        col = db_conn[colname]
        try:
            col.drop()
            logging.debug("resetting %s[%s] = success to drop collection" % (dbname,colname))
            return True
        except:
            logging.debug("resetting %s[%s] = failed to drop collection" % (dbname,colname))
            return False

    def insert_msg(self, msg, dbname=None, colname=None, update=False, _id=None):
        _id = self.extract_id_key(msg) if _id is None else _id
        return self.insert(nmsg, dbname, colname, update=update, _id=_id)

    def extract_id_key(self, content):
        keys = self.id_keys if not self.create_id else self.data_hash_keys
        if self.create_id and self.data_hash_keys is None:
            res = json.dumps(content,
                             sort_keys=True,
                             separators=(',', ': '))
            return self.gen_hash_key(res)

        _id = None
        if keys is None:
            return _id

        pos = 0
        lvl = content
        for k in keys:
            lvl = lvl[k] if isinstance(lvl, dict) and k in lvl else None
            if lvl is None:
                break
            elif len(keys)-1 == pos and lvl is not None and self.create_id:
                _id = self.gen_hash_key(lvl)
                break
            elif len(keys)-1 == pos and lvl is not None:
                _id = lvl
                break
            pos += 1
        return _id

    def insert(self, content, dbname, colname, update=False, _id=None):
        _id = self.extract_id_key(content) if _id is None else _id
        return self.inserts([content,], dbname, colname, update=update)

    def inserts(self, contents, dbname, colname, update=False):
        results = []
        inserted = 0
        contents_ids_tuples= [(i, self.extract_id_key(i)) for i in contents]
        for c, _id in contents_ids_tuples:
            r = self._insert(c, dbname=dbname, colname=colname, update=update, _id=_id)
            if r[0]:
                inserted += 1
            results.append(r)
        logging.debug("Inserted %d items in %s[%s]"%(inserted, dbname, colname))
        return results

    def get_col_conn(self, dbname=None, colname=None):
        colname = self.colname if colname is None else colname
        dbname = self.dbname if dbname is None else dbname
        db_conn = self._conn[dbname]
        return db_conn[colname]

    def is_empty(self, dbname, colname):
        return not self.has_obj({}, dbname, colname)

    def has_obj(self, obj_dict, dbname, colname, col_conn=None):
        x = self.get_one(dbname=dbname, colname=colname, obj_dict=obj_dict)
        # logging.debug("%s[%s] has_obj '%s' == %s"%(dbname, colname, obj_dict, x is not None))
        if x is None or len(x) == 0:
            return False
        return True

    def get_all(self, dbname=None, colname=None, obj_dict={}):
        colname = self.colname if colname is None else colname
        dbname = self.dbname if dbname is None else dbname
        #print dbname, colname
        db_conn = self._conn[dbname]
        col_conn = db_conn[colname]
        try:
            x = col_conn.find(obj_dict)
            return [i for i in x]
        except:
            raise
        return None

    def get_one(self, dbname=None, colname=None, obj_dict={}):
        colname = self.colname if colname is None else colname
        dbname = self.dbname if dbname is None else dbname
        db_conn = self._conn[dbname]
        col_conn = db_conn[colname]
        try:
            x = col_conn.find_one(obj_dict)
            return x
        except:
            pass
        return None

    def _insert(self, content, dbname=None, colname=None, update=False, _id=None):
        _id = self.extract_id_key(content) if _id is None else _id
        colname = self.colname if colname is None else colname
        dbname = self.dbname if dbname is None else dbname
        db_conn = self._conn[dbname]
        #print dbname, colname, update, _id
        if update and _id is not None and \
           self.has_obj({'_id': _id}, dbname, colname):
           db_conn[colname].replace_one({'_id': _id}, content, bypass_document_validation=True)
           return True, _id
        elif _id is not None and \
           self.has_obj({'_id': _id}, dbname, colname):
           return False, _id
        c = content.copy()
        if _id is not None:
            c['_id'] = _id
        return True, db_conn[colname].insert_one(c, bypass_document_validation=True).inserted_id
