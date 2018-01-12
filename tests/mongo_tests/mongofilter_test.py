from datetime import datetime
import unittest
from unittest import TestCase
import toml
from fiery_snap.impl.processor.mongo_filter_processor import MongoFilterProcessor, MongoClientImpl


INSERT_EXAMPLE_TOML = '''
uri = 'mongodb://127.0.0.1:27017'
name = "filter-test"
dbname = 'default'
colname = 'default'
id_key = 'testid'
data_hash_key = 'content'
[service]
    name = 'mongo_filter_service'
    listening_port = 17878
    listening_address = '0.0.0.0'
'''

EXAMPLE_TOML = '''
[filters.twitter_filter]
name = 'twitter_filter'
uri = 'mongodb://127.0.0.1:27017'
dbname = 'default'
colname = 'default'
id_key = 'content_id'
data_hash_key = 'content'
[filters.twitter_filter.service]
    name = 'mongo_filter_service'
    listening_port = 17878
    listening_address = '0.0.0.0'
[filters.twitter_filter.publishers.unfiltered_test_feed]
    name = 'redis_queue'
    queue_name = "unfiltered_test_feed"
    uri = "redis://0.0.0.0:6379"

[filters.twitter_filter.subscribers.test_feed]
    name = 'test_feed'
    queue_name = "test_feed"
    uri = "redis://0.0.0.0:6379"
    limit = 10000
'''
TEST_MSGS_WITH_ID_UNIQ = 6
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

TEST_WITH_ID_UNIQ = 7
TEST_WITH_ID = [ 
    {'testid':'a', 'content': '1', 'msg':1, 'filtered': False},
    {'testid':'b', 'content': '2', 'msg':2, 'filtered': False},
    {'testid':'c', 'content': '3', 'msg':3, 'filtered': False},
    {'testid':'d', 'content': '4', 'msg':4, 'filtered': False},
    {'testid':'b', 'content': '1', 'msg':1+5, 'filtered': True},
    {'testid':'c', 'content': '2', 'msg':2+5, 'filtered': True},
    {'testid':'d', 'content': '3', 'msg':3+5, 'filtered': True},
    {'testid':'e', 'content': '4', 'msg':4+5, 'filtered': False},
    {'testid':'A', 'content': '6', 'msg':10, 'filtered': False},
    {'content': '5', 'msg':5, 'filtered': False},
    {'content': '5', 'msg':5+5, 'filtered': True},
]

NS_TEST_WITH_ID_UNIQ = 9
NS_TEST_WITH_ID = [ 
    {'testid':None, 'content': '1', 'msg':1, 'filtered': False},
    {'testid':int(1), 'content': '2', 'msg':2, 'filtered': False},
    {'testid':datetime.now(), 'content': '3', 'msg':3, 'filtered': False},
    {'testid':'d', 'content': '4', 'msg':4, 'filtered': False},
    {'testid':'b', 'content': '1', 'msg':1+5, 'filtered': False},
    {'testid':'c', 'content': '2', 'msg':2+5, 'filtered': False},
    {'testid':'d', 'content': '3', 'msg':3+5, 'filtered': True},
    {'testid':'e', 'content': '4', 'msg':4+5, 'filtered': False},
    {'testid':'A', 'content': '6', 'msg':10, 'filtered': False},
    {'content': '5', 'msg':5, 'filtered': False},
    {'content': '5', 'msg':5+5, 'filtered': True},
]

NS_TEST_MIXED_UNIQ = 9
NS_TEST_MIXED_ID = [ 
    {'testid':None, 'content': None, 'msg':1, 'filtered': False},
    {'content': int(1), 'msg':2, 'filtered': False},
    {'content': datetime.now(), 'msg':3, 'filtered': False},
    {'testid':'d', 'content': '4', 'msg':4, 'filtered': False},
    {'testid':'b', 'content': '1', 'msg':1+5, 'filtered': False},
    {'testid':'c', 'content': '2', 'msg':2+5, 'filtered': False},
    {'testid':'d', 'content': '3', 'msg':3+5, 'filtered': True},
    {'testid':'e', 'content': '4', 'msg':4+5, 'filtered': False},
    {'testid':'A', 'content': '6', 'msg':10, 'filtered': False},
    {'content': '5', 'msg':5, 'filtered': False},
    {'content': '5', 'msg':5+5, 'filtered': True},
]

class TestMongoFilter(unittest.TestCase):
    def test_parseblock(self):
        config_dict = toml.loads(EXAMPLE_TOML)
        mfp_config = config_dict.get('filters', {}).get('twitter_filter', {})
        self.assertTrue(len(mfp_config) > 0)
        mfp = MongoFilterProcessor.parse(mfp_config)
        self.assertTrue(mfp.name == 'twitter_filter')
        self.assertTrue(mfp.dbname == u'default')
        self.assertTrue(mfp.colname == u'default')
        self.assertTrue(mfp.db_conn.dbname == 'default')
        self.assertTrue(mfp.db_conn.colname == 'default')
        self.assertTrue(mfp.id_key == 'content_id')
        self.assertTrue(mfp.data_hash_key == 'content')

    def test_parseblock2(self):
        config_dict = toml.loads(INSERT_EXAMPLE_TOML)
        mfp = MongoFilterProcessor.parse(config_dict)
        self.assertTrue(isinstance(mfp, MongoFilterProcessor))
        self.assertTrue(isinstance(mfp.db_conn, MongoClientImpl))
        self.assertTrue(mfp.name == 'filter-test')
        self.assertTrue(mfp.dbname == u'default')
        self.assertTrue(mfp.colname == u'default')
        self.assertTrue(mfp.db_conn.dbname == 'default')
        self.assertTrue(mfp.db_conn.colname == 'default')
        self.assertTrue(mfp.id_key == 'testid')
        self.assertTrue(mfp.data_hash_key == 'content')

    def test_insertblock2(self):
        config_dict = toml.loads(INSERT_EXAMPLE_TOML)
        mfp = MongoFilterProcessor.parse(config_dict)
        mfp.reset()

        
        r = mfp.insert_msgs(TEST_WITH_ID)
        msgs = mfp.get_all()
        self.assertTrue(len(msgs) == TEST_WITH_ID_UNIQ)
        for m in msgs:
            self.assertFalse(m['filtered'])

    def test_insertblock(self):
        config_dict = toml.loads(INSERT_EXAMPLE_TOML)
        mfp = MongoFilterProcessor.parse(config_dict)
        mfp.reset()
        
        r = mfp.insert_msgs(TEST_MSGS)
        msgs = mfp.get_all()
        self.assertTrue(len(msgs) == TEST_MSGS_WITH_ID_UNIQ)
        for m in msgs:
            self.assertFalse(m['filtered'])

    def test_insertblock3(self):
        config_dict = toml.loads(INSERT_EXAMPLE_TOML)
        mfp = MongoFilterProcessor.parse(config_dict)
        mfp.reset()
        
        r = mfp.insert_msgs(NS_TEST_WITH_ID)
        msgs = mfp.get_all()
        self.assertTrue(len(msgs) == NS_TEST_WITH_ID_UNIQ)
        for m in msgs:
            self.assertFalse(m['filtered'])

    def test_insertblock4(self):
        config_dict = toml.loads(INSERT_EXAMPLE_TOML)
        mfp = MongoFilterProcessor.parse(config_dict)
        mfp.reset()
        
        r = mfp.insert_msgs(NS_TEST_MIXED_ID)
        msgs = mfp.get_all()
        self.assertTrue(len(msgs) == NS_TEST_MIXED_UNIQ)
        for m in msgs:
            self.assertFalse(m['filtered'])

if __name__ == '__main__':
    unittest.main()
