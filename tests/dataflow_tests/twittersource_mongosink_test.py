import requests
import unittest
import toml
import time

from fiery_snap.parse import ConfigParser, INPUTS, CLASS_MAPS
from fiery_snap.impl.util.page import TestPage, JsonUploadPage
from fiery_snap.impl.output.mongo_sink import MongoStore, MongoClientImpl
from fiery_snap.impl.util.page import Page, AddHandlesPage, \
               ListHandlesPage, RemoveHandlesPage, ConsumePage, \
               JsonUploadPage, TestPage, MongoSearchPage

from .twitter_data import get_json_data as get_twtr_msgs, TWITTER_DATA
import logging
import time
import requests
import unittest
from unittest import TestCase



TS_MS_CONFIG = '''
[inputs.twitter-source]
    name = 'twitter-source'
    type = 'simple-in-twitter'
    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_token_secret = ''

    handles = ['nao_sec', 'malwrhunterteam', 'Racco42', 'SecGuru', 'illegalFawn', 'dvk01uk', 'Ring0x0', 
               'James_inthe_box', 'azsxdvfbg', 'VK_Intel', 'Stormshield', 'Antelox', 'DynamicAnalysis',
               'thlnk3r', 'BroadAnalysis', 'jeromesegura', 'w1mp1k1ng', 'malc0de', 'EKwatcher', 'Kafeine',
               'Zerophage1337', 'baberpervez2']

    [inputs.twitter-source.service]
        name = 'twitter-source_service'
        listening_port = 19003
        listening_address = '0.0.0.0'
    [inputs.twitter-source.subscribers.redis_queue]
        name = 'unfiltered_twitter_feed'
        queue_name = "unfiltered_twitter_feed"
        uri = "redis://0.0.0.0:6379"
        limit = 10000 # adjustable limit 10 posts per request for content
    [inputs.twitter-source.subscribers.ms-raw-twitter]
        name = 'ms-raw-twitter'
        queue_name = "ms-raw-twitter"
        uri = "redis://0.0.0.0:6379"
        limit = 10000 # adjustable limit 10 posts per request for content

[outputs.mongo-sink-raw]
    uri = 'mongodb://0.0.0.0:27017'
    name = "mongo-test"
    dbname = 'twitter-feeds'
    colname = 'raw-tweets'
    type = 'simple-mongo-store'

    [outputs.mongo-sink-raw.service]
        name = 'mongo-sink-raw'
        listening_port = 19004
        listening_address = '0.0.0.0'

    [outputs.mongo-sink-raw.publishers.ms-raw-twitter]
        name = 'ms-raw-twitter'
        queue_name = "ms-raw-twitter"
        uri = "redis://0.0.0.0:6379"
'''

MS_NAME = 'mongo-sink-raw'
TS_NAME = 'twitter-source'
INPUTS = 'inputs'
OUTPUTS = 'outputs'

# these will be set by main
class TwitterSourceConfig(object):
    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_token_secret = ''


class TSMSFlowTest(unittest.TestCase):

    def parseConfig(self):
        toml_dict = toml.loads(TS_MS_CONFIG)
        toml_dict[INPUTS][TS_NAME]['consumer_key'] = TwitterSourceConfig.consumer_key
        toml_dict[INPUTS][TS_NAME]['consumer_secret'] = TwitterSourceConfig.consumer_secret
        toml_dict[INPUTS][TS_NAME]['access_token'] = TwitterSourceConfig.access_token
        toml_dict[INPUTS][TS_NAME]['access_token_secret'] = TwitterSourceConfig.access_token_secret
        results = ConfigParser.parse_components_dict(toml_dict)
        return results

    def test_parse(self):
        components_dict = self.parseConfig()
        self.assertTrue(MS_NAME in components_dict[OUTPUTS]) 
        self.assertTrue(TS_NAME in components_dict[INPUTS]) 

    def extract_components(self, results):
        components = {}
        # all the inputs
        for name, item in results[INPUTS].items():
            components[name] = item

        # all the outputs
        for name, item in results[OUTPUTS].items():
            components[name] = item

        return components

    def startup_component_svcs(self, components_dict):
        for name, item in components_dict.items():
            logging.debug("Starting consumer: %s" % name)
            # item.periodic_consume(2.0)
            logging.debug("Starting %s web backend" % name)
            started = item.svc.start()
            self.assertTrue(started)
            self.assertTrue(item.svc.is_alive())

    def shutdown_component_svcs(self, components_dict):
        for name, item in components_dict.items():
            logging.debug("Stopping %s web backend" % name)
            item.svc.stop()
            logging.debug("... %s web backend is alive %s" % (name, item.svc.is_alive()))
            self.assertFalse(item.svc.is_alive())

    def test_start_stop(self):
        results = self.parseConfig()
        components = self.extract_components(results)
        self.startup_component_svcs(components)
        time.sleep(2.0)
        self.shutdown_component_svcs(components)


    def test_start_stop2(self):
        results = self.parseConfig()
        components = self.extract_components(results)
        self.startup_component_svcs(components)
        time.sleep(2.0)
        self.shutdown_component_svcs(components)

    def ingest_twitter_data(self, twitter_source):
        twitter_data = get_twtr_msgs()
        msgs = twitter_data[:200]
        twitter_source.reset_all()
        ts_json = twitter_source.svc.get_base_url(JsonUploadPage.NAME)
        logging.debug("Sending %d msgs to %s: %s" % (len(msgs), twitter_source.name, ts_json))
        r = requests.post(ts_json, json={'msgs':msgs, 'target':twitter_source.svc.name})
        logging.debug("JsonUpload request result: %s" % (r.text))
        self.assertTrue(r.status_code == 200)

    def trigger_mongo_consume(self, mongo_sink):
        mongo_sink.reset()
        ts_json = mongo_sink.svc.get_base_url(ConsumePage.NAME)
        logging.debug("Sending request to %s: %s" % (mongo_sink.name, ts_json))
        r = requests.post(ts_json, json={'target':mongo_sink.svc.name})
        self.assertTrue(r.status_code == 200)
        logging.debug("Consume request result: %s" % (r.text))

    def check_mongo(self, mongo_sink):
        query = mongo_sink.svc.get_base_url(MongoSearchPage.NAME)
        logging.debug("Sending request to %s: %s" % (mongo_sink.name, query))
        q = requests.post(query, json={'query':{}, 'target':mongo_sink.svc.name})
        # logging.debug("MongoSearchPage result: %s"%q.text)
        self.assertTrue(q.status_code == 200)
        self.assertTrue(len(q.json()) == 200)

    def test_json_twitter_ingest(self):
        results = self.parseConfig()
        components = self.extract_components(results)
        self.startup_component_svcs(components)
        tw_src = components.get(TS_NAME)
        ms_sink = components.get(MS_NAME)
        try:
            self.ingest_twitter_data(tw_src)
            time.sleep(2.0)
            self.trigger_mongo_consume(ms_sink)
            self.check_mongo(ms_sink)
        except:
            raise
        finally:
            self.shutdown_component_svcs(components)




# TODO add flow construction code
# Load tweets, upload to TS, and then check MS after N seconds

if __name__ == '__main__':
    unittest.main()
