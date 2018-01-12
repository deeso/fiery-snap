import logging
import time
import requests
import unittest
from unittest import TestCase
import toml
from fiery_snap.impl.output.mongo_sink import MongoStore, MongoClientImpl
from fiery_snap.impl.util.page import Page, AddHandlesPage, \
               ListHandlesPage, RemoveHandlesPage, ConsumePage, \
               JsonUploadPage, TestPage, MongoSearchPage


EXAMPLE_TEST = '''
uri = 'mongodb://127.0.0.1:27017'
name = "mongo-test"
dbname = 'test'
colname = 'test'
id_key = 'testid'
[service]
    name = 'mongo_service'
    listening_port = 17880
    listening_address = '0.0.0.0'
[publishers.ms-raw-twitter-test]
    name = 'ms-raw-twitter-test'
    queue_name = "ms-raw-twitter-test"
    uri = "redis://127.0.0.1:6379"

'''

TEST_MSGS = [ 
    {'content': '1', 'msg':1, 'filtered': False},
    {'content': '2', 'msg':2, 'filtered': False},
    {'content': '3', 'msg':3, 'filtered': False},
    {'content': '4', 'msg':4, 'filtered': False},
    {'content': '5', 'msg':5, 'filtered': False},
    {'content': '1', 'msg':1+5, 'filtered': True},
    {'content': '2', 'msg':2+5, 'filtered': True},
    {'content': '3', 'msg':3+5, 'filtered': True},
    {'content': '4', 'msg':4+5, 'filtered': True},
    {'content': '5', 'msg':5+5, 'filtered': True},
    {'content': '6', 'msg':10, 'filtered': False},
]

class TestMongoConnection(unittest.TestCase):
    # def test_parseblock(self):
    #     config_dict = toml.loads(EXAMPLE_TOML)
    #     ms = MongoStore(config_dict)        
    #     self.assertTrue(isinstance(ms, MongoStore))
    #     self.assertTrue(isinstance(ms.db_conn, MongoClientImpl))
    #     self.assertTrue(ms.name == 'filter-test')
    #     self.assertTrue(ms.config.get('dbname') == u'default')
    #     self.assertTrue(ms.config.get('colname') == u'default')
    #     self.assertTrue(ms.db_conn.dbname == 'default')
    #     self.assertTrue(ms.db_conn.colname == 'default')
    #     self.assertTrue(ms.config.get('id_key') == 'testid')

    def test_parseblock(self):
        config_dict = toml.loads(EXAMPLE_TEST)
        ms = MongoStore(config_dict)        
        self.assertTrue(isinstance(ms, MongoStore))
        self.assertTrue(isinstance(ms.db_conn, MongoClientImpl))
        self.assertTrue(ms.name == 'mongo-test')
        self.assertTrue(ms.config.get('dbname') == u'test')
        self.assertTrue(ms.config.get('colname') == u'test')
        self.assertTrue(ms.db_conn.dbname == 'test')
        self.assertTrue(ms.db_conn.colname == 'test')
        ms.reset()

    def test_jsonupload(self):
        config_dict = toml.loads(EXAMPLE_TEST)
        ms = MongoStore(config_dict)        
        started = ms.svc.start()
        self.assertTrue(started)
        self.assertTrue(ms.svc.is_alive())
        time.sleep(2.0)
        ms.reset()
        json_upload = ms.svc.get_base_url(JsonUploadPage.NAME)
        query = ms.svc.get_base_url(MongoSearchPage.NAME)

        msg_batch_1 = TEST_MSGS[0:3] 
        msg_batch_2 = TEST_MSGS[3:5]
        msg_batch_2_msg = TEST_MSGS[6]
        r = requests.post(json_upload, json={'entries':msg_batch_1, 
                                             'update': False,
                                             # skip the queue and put directly into DB
                                             'direct': True,
                                             'target': ms.svc.name})
        self.assertTrue(r.status_code == 200)
        logging.debug("JsonUploadPage result: %s"%r.text)

        q = requests.post(query, json={'query':{}, 'target': ms.svc.name})
        logging.debug("MongoSearchPage result: %s"%q.text)
        self.assertTrue(q.status_code == 200)
        self.assertTrue(len(q.json()) == 3)
        qs_state = ms.is_empty()
        self.assertFalse(qs_state['test[test]'])
        ms.reset()
        qs_state = ms.is_empty()
        self.assertTrue(qs_state['test[test]'])
        ms.svc.stop()
        time.sleep(2.0)
        self.assertFalse(ms.svc.is_alive())

    def test_indirect_insert(self):
        config_dict = toml.loads(EXAMPLE_TEST)
        ms = MongoStore(config_dict)        
        started = ms.svc.start()
        self.assertTrue(started)
        self.assertTrue(ms.svc.is_alive())
        time.sleep(2.0)
        ms.reset()
        qs_state = ms.is_empty()
        self.assertTrue(qs_state['test[test]'])
        json_upload = ms.svc.get_base_url(JsonUploadPage.NAME)
        query = ms.svc.get_base_url(MongoSearchPage.NAME)
        consume = ms.svc.get_base_url(ConsumePage.NAME)
        
        msg_batch_1 = TEST_MSGS[0:3] 
        r = requests.post(json_upload, json={'entries':msg_batch_1, 
                                             'update': False,
                                             # place into queue for consumption
                                             'direct': False,
                                             'target': ms.svc.name})
        self.assertTrue(r.status_code == 200)
        logging.debug("JsonUploadPage result: %s"%r.text)
        r = requests.post(consume, json={'target': ms.svc.name})
        self.assertTrue(r.status_code == 200)
        logging.debug("ConsumePage result: %s"%r.text)

        q = requests.post(query, json={'query':{}, 'target': ms.svc.name})
        logging.debug("MongoSearchPage result: %s"%q.text)
        self.assertTrue(q.status_code == 200)
        self.assertTrue(len(q.json()) == 3)
        qs_state = ms.is_empty()
        self.assertFalse(qs_state['test[test]'])
        ms.reset()
        qs_state = ms.is_empty()
        self.assertTrue(qs_state['test[test]'])
        ms.svc.stop()
        time.sleep(2.0)
        self.assertFalse(ms.svc.is_alive())
        

if __name__ == '__main__':
    unittest.main()

