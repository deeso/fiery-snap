import requests
import unittest
import toml
import time

from fiery_snap.parse import ConfigParser, INPUTS, CLASS_MAPS, PROCESSORS, OUTPUTS, TAGS, OSINT
from fiery_snap.impl.util.page import TestPage, JsonUploadPage
from fiery_snap.impl.output.mongo_sink import MongoStore, MongoClientImpl
from fiery_snap.impl.util.page import Page, AddHandlesPage, \
               ListHandlesPage, RemoveHandlesPage, ConsumePage, \
               JsonUploadPage, TestPage, MongoSearchPage

from fiery_snap.impl.processor.twitter_scraper_processor import DEBUG_CONTENT

from fiery_snap.impl.util.page import TestPage, JsonUploadPage
from fiery_snap.io.message import Message
from fiery_snap.io.connections import LogstashJsonUDPConnection

import inspect
import types
import base64
import json
import traceback
import unittest
import requests
import regex
import toml
import sys
import logging
import time

from .twitter_data import get_json_data as get_twtr_msgs, TWITTER_DATA
from .twitterscraper_test_messages_config import TEST_CASES, C, V, TEST_CASES_ID_MAPPING


TS_MS_TP_CONFIG = '''
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
        name = 'raw_twitter_feed'
        queue_name = "raw_twitter_feed"
        uri = "redis://0.0.0.0:6379"
        limit = 10000 # adjustable limit 10 posts per request for content

    [inputs.twitter-source.subscribers.ms-raw-twitter]
        name = 'ms-raw-twitter'
        queue_name = "ms-raw-twitter"
        uri = "redis://0.0.0.0:6379"
        limit = 10000 # adjustable limit 10 posts per request for content

[outputs.mongo-sink-raw]
    uri = 'mongodb://0.0.0.0:27017'
    name = "mongo-sink-raw"
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

[outputs.mongo-sink-processed]
    uri = 'mongodb://0.0.0.0:27017'
    name = "mongo-sink-processed"
    dbname = 'twitter-feeds'
    colname = 'processed-tweets'
    type = 'simple-mongo-store'

    [outputs.mongo-sink-processed.service]
        name = 'mongo-sink-processed'
        listening_port = 19005
        listening_address = '0.0.0.0'

    [outputs.mongo-sink-processed.publishers.ms-processed-twitter]
        name = 'ms-processed-twitter'
        queue_name = "ms-processed-twitter"
        uri = "redis://0.0.0.0:6379"

[osint.tags]
    afraid = '^afraid.*'
    afriad = '^afriad.*'
    angler = '^angler.*'
    bandarchore = '^bandarchore.*'
    cerber = '^cerber.*'
    chthonic = '^chthonic.*'
    crypmic = '^crypmic.*'
    cryptmic = '^cryptmic.*'
    cryptxxx = '^cryptxxx.*'
    eitest = '^eitest.*'
    goodman = '^goodman.*'
    gootkit = '^gootkit.*'
    kaixin = '^kaixin.*'
    magnitude = '^magnitude.*'
    magnitudeek = '^magnitudeek.*'
    nebula = '^nebula.*'
    neurtrino = '^neurtrino.*'
    neutrino = '^neutrino.*'
    pseudo = '^pseudo.*'
    qbot = '^qbot.*'
    ramnit = '^ramnit.*'
    realstatistics = '^realstatistics.*'
    rig = '^rig.*'
    rulan = '^rulan.*'
    seamless = '^seamless.*'
    sundown = '^sundown.*'
    terror = '^terror.*'
    teslacrypt = '^teslacrypt.*'
    badrabbit = '^badrabbit.*'
    locky = '^locky.*'
    pandabanker = '^pandabankder.*'
    smokeloader = '^smokeloader.*'

[processors.twitter-scraper]
    name = 'twitter-scraper'
    type = 'twitterscraper'
    subscriber_polling = 10.0 # control the periods between polls
    message_count = 100 # limit the number of messages processed at a time
    simple_msg = "From:{source_type}:{source}->{processor}:{processor_type}\\n{safe_hosts} {tags}\\n(credit {user}: {link})"

    [processors.twitter-scraper.service]
        name = 'twitter-scraper-service'
        listening_port = 19006
        listening_address = '0.0.0.0'

    [processors.twitter-scraper.publishers.raw_twitter_feed]
        name = 'raw_twitter_feed'
        queue_name = "raw_twitter_feed"
        uri = "redis://0.0.0.0:6379"

    [processors.twitter-scraper.subscribers.ms-processed-twitter]
        name = 'ms-processed-twitter'
        queue_name = "ms-processed-twitter"
        uri = "redis://0.0.0.0:6379"

    [processors.twitter-scraper.subscribers.local]
        name = 'local'
        uri = ""
'''

TP_NAME = 'twitter-scraper'
MP_NAME = 'mongo-sink-processed'
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

class FullPipelineTest(unittest.TestCase):
    def convert_content_to_msg(self, twitter_msgs):
        return [Message(i) for i in twitter_msgs]
    
    def read_twitter_data(self):
        twitter_msgs = get_twtr_msgs()
        self.myassertTrue(len(TWITTER_DATA) == len(twitter_msgs))
        return twitter_msgs
        
    def parseConfig(self):
        toml_dict = toml.loads(TS_MS_TP_CONFIG)
        toml_dict[INPUTS][TS_NAME]['consumer_key'] = TwitterSourceConfig.consumer_key
        toml_dict[INPUTS][TS_NAME]['consumer_secret'] = TwitterSourceConfig.consumer_secret
        toml_dict[INPUTS][TS_NAME]['access_token'] = TwitterSourceConfig.access_token
        toml_dict[INPUTS][TS_NAME]['access_token_secret'] = TwitterSourceConfig.access_token_secret
        results = ConfigParser.parse_components_dict(toml_dict)
        return results

    def test_parse(self):
        components_dict = self.parseConfig()
        self.assertTrue(MS_NAME in components_dict[OUTPUTS]) 
        self.assertTrue(MP_NAME in components_dict[OUTPUTS]) 
        self.assertTrue(TS_NAME in components_dict[INPUTS])
        self.assertTrue(TP_NAME in components_dict[PROCESSORS])

    
    def extract_components(self, results):
        components = {}
        # all the processors
        for name, item in results[PROCESSORS].items():
            components[name] = item
        for name, item in results[OUTPUTS].items():
            components[name] = item
        for name, item in results[INPUTS].items():
            components[name] = item
        return components

    def reset_component_queues(self, components):
        for name, item in components.items():
            logging.debug("Reseting consumer queues: %s" % name)
            item.reset_all()
            qs_state = item.is_empty()
            for qname, is_empty in qs_state.items():
                logging.debug("%s queue %s is empty: %s"%(name, qname, is_empty))
                self.assertTrue(is_empty)
            # item.periodic_consume(2.0)
            logging.debug("Reseting %s queues complete" % name)
        logging.debug("Reseting components completed")

    
    def startup_component_svcs(self, components_dict):
        for name, item in components_dict.items():
            logging.debug("Starting consumer: %s" % name)
            # item.periodic_consume(2.0)
            logging.debug("Starting %s web backend" % name)
            started = item.svc.start()
            self.myassertTrue(started)
            self.myassertTrue(item.svc.is_alive())
    
    def shutdown_component_svcs(self, components_dict):
        for name, item in components_dict.items():
            logging.debug("Stopping %s web backend" % name)
            item.svc.stop()
            logging.debug("... %s web backend is alive %s" % (name, item.svc.is_alive()))
            self.myassertFalse(item.svc.is_alive())

    def test_start_stop(self):
        results = self.parseConfig()
        components = self.extract_components(results)
        self.startup_component_svcs(components)
        time.sleep(2.0)
        self.shutdown_component_svcs(components)
    
    def test_parse(self):
        components_dict = self.parseConfig()
        self.myassertTrue(TP_NAME in components_dict[PROCESSORS]) 
    
    def myassertTrue(self, condition):
        # if not isinstance(self, types.ClassType):
        self.assertTrue(condition)     
    
    def myassertFalse(self, condition):
        if not isinstance(self, types.ClassType):
            self.assertFalse(condition) 
    
    def compare_values(self, e_value, a_value):
        self.myassertTrue(type(e_value) == type(a_value))
        # handle case for set, dict, list, and value(str, int)
        if isinstance(e_value, set) or isinstance(e_value, list):
            self.myassertTrue(len(e_value) == len(a_value))
            for a, b in zip(sorted(e_value), sorted(a_value)):
                self.myassertTrue(a == b)
        elif isinstance(e_value, dict):
            # self.myassertTrue(len(e_value), len(a_value))
            for key, item in e_value.items():
                self.myassertTrue(key in a_value)
                self.myassertTrue(item == a_value[key])
        else:
            self.myassertTrue(a_value == e_value)
    
    def walk_results(self, expected_results, actual_results, startfrom=None):
        for t, v in expected_results:
            cur = actual_results if startfrom is None else startfrom
            self.myassertTrue(t in [C, V])
            # one name per check
            self.myassertTrue(len(v.keys()) == 1)
            # when t == V: v is the value of the expected result
            # when t == C: c is the expected results, a list of (name, value)
            # and value may be a C (check) or V (concrete value)
            if t == V:
                # extract the value name
                valuekey = v.keys()[0]
                e_value = v[valuekey]
                # check that the expected valus is present and check the value
                self.myassertTrue(valuekey is not None and cur is not None)
                self.myassertTrue(valuekey in cur)
                self.compare_values(e_value, cur[valuekey])
            else:
                # get the name of the field we want to check
                typekey = v.keys()[0]
                self.myassertTrue(typekey in cur)
                cur = cur[typekey]
                self.walk_results(v.values()[0], actual_results, cur)
    
    def compare_results(self, test_case, actual_results):
        expected_results = test_case['results']
        self.walk_results(expected_results, actual_results)

    # def test_send_empty_json(self):
    #     messages = self.convert_content_to_msg(self.read_twitter_data())
    #     results = self.parseConfig()
    #     components = self.extract_components(results)
    #     self.startup_component_svcs(components)
    #     try:
    #         time.sleep(2.0)
    #         for c in components.values():
    #             logging.debug("Sending Empty JSON request to from %s"%(c.svc.name))
    #             location = c.svc.get_base_url(JsonUploadPage.NAME)
    #             r = requests.post(location, json={'target': c.svc.name})
    #             self.assertTrue(r.status_code == 200)
    #             logging.debug("Empty JSON response from %s: %s"%(c.svc.name, r.text))
    #     except:
    #         raise
    #     finally:
    #         self.shutdown_component_svcs(components)

    def inject_twitter_source_messages(self, twitter_source, msgs):
        qs_state = twitter_source.is_empty()
        self.assertTrue('raw_twitter_feed' in qs_state)
        self.assertTrue('ms-raw-twitter' in qs_state)
        self.assertTrue(qs_state['raw_twitter_feed'])
        location = twitter_source.svc.get_base_url(JsonUploadPage.NAME)
        r = requests.post(location, json={'msgs':msgs, 
                                          'target': twitter_source.svc.name})
        self.assertTrue(r.status_code == 200)
        qs_state = twitter_source.is_empty()
        self.assertFalse(qs_state['raw_twitter_feed'])
        self.assertFalse(qs_state['ms-raw-twitter'])
        # DONT Do this it consumes from twitter not the internal TS queue
        # location = twitter_source.svc.get_base_url(ConsumePage.NAME)
        # r = requests.post(location, json={'target': twitter_source.svc.name}) 
        # self.assertTrue(r.status_code == 200)
        # logging.debug("Consume response from %s: %s"%(twitter_source.name, r.text))
        # qs_state = twitter_source.is_empty()
        # self.assertTrue(qs_state['raw_twitter_feed'])

    def consume_twitter_scraper_messages(self, twitter_scraper):
        qs_state = twitter_scraper.is_empty()
        logging.debug("Logging %s is alive: %s"% (twitter_scraper.svc.name, twitter_scraper.svc.t.is_alive()))
        self.assertTrue('raw_twitter_feed' in qs_state)
        self.assertTrue('ms-processed-twitter' in qs_state)
        self.assertFalse(qs_state['raw_twitter_feed'])
        location = twitter_scraper.svc.get_base_url(ConsumePage.NAME)
        r = requests.post(location, json={'target': twitter_scraper.svc.name}) 
        self.assertTrue(r.status_code == 200)
        logging.debug("Consume response from %s: %s"%(twitter_scraper.name, r.text))
        qs_state = twitter_scraper.is_empty()
        self.assertTrue(qs_state['raw_twitter_feed'])
        self.assertFalse(qs_state['ms-processed-twitter'])

    def consume_mongo_sink_processed_messages(self, mongo_sink_processed):
        db = '%s[%s]' % (mongo_sink_processed.config['dbname'], mongo_sink_processed.config['colname'])
        qs_state = mongo_sink_processed.is_empty()
        self.assertFalse(qs_state['ms-processed-twitter'])
        location = mongo_sink_processed.svc.get_base_url(ConsumePage.NAME)
        r = requests.post(location, json={'target': mongo_sink_processed.svc.name}) 
        self.assertTrue(r.status_code == 200)
        logging.debug("Consume response from %s: %s"%(mongo_sink_processed.name, r.text))
        qs_state = mongo_sink_processed.is_empty()
        self.assertTrue(qs_state['ms-processed-twitter'])
        self.assertTrue(db in qs_state)
        self.assertFalse(qs_state[db])

    def consume_mongo_sink_raw_messages(self, mongo_sink_raw):
        db = '%s[%s]' % (mongo_sink_raw.config['dbname'], mongo_sink_raw.config['colname'])
        qs_state = mongo_sink_raw.is_empty()
        print mongo_sink_raw.publishers
        print mongo_sink_raw.subscribers
        print qs_state
        self.assertTrue('ms-raw-twitter' in qs_state)
        self.assertFalse(qs_state['ms-raw-twitter'])
        location = mongo_sink_raw.svc.get_base_url(ConsumePage.NAME)
        r = requests.post(location, json={'target': mongo_sink_raw.svc.name}) 
        self.assertTrue(r.status_code == 200)
        logging.debug("Consume response from %s: %s"%(mongo_sink_raw.name, r.text))
        qs_state = mongo_sink_raw.is_empty()
        self.assertTrue(qs_state['ms-raw-twitter'])
        self.assertFalse(qs_state[db])

    def get_expected_results(self, mongo_results):
        self.assertTrue('meta' in mongo_results)
        self.assertTrue('id' in mongo_results['meta'])
        id_ = mongo_results['meta']['id']
        self.assertTrue(id_ in TEST_CASES_MAPPINGS)
        test_case_name = TEST_CASES_MAPPINGS[id_]
        self.assertTrue(test_case_name in TEST_CASES)
        test_case = TEST_CASES[test_case_name]
        return test_case['results']

    def process_some_tweets(self, components):
        self.reset_component_queues(components)
        twitter_scraper = components[TP_NAME]
        twitter_source = components[TS_NAME]
        mongo_sink_processed = components[MP_NAME]
        mongo_sink_raw = components[MS_NAME]

        # testing for functionality not precision or accuracy here
        # so consuming messages, injecting them into
        # the twitter-source and looking at them as they
        # come out of the twitter-scraper or mongo-processed
        # For accuracy use TEST_CASES_MAPPINGS to match ID of results
        # with test case name, to get the expected results
        injects = []
        for name, test_case in TEST_CASES.items():
            logging.debug("Injecting message into %s: %s" % (twitter_source.name, name))
            self.myassertTrue(len(set(['msg', 'results']) & set(test_case.keys())) == 2)
            injects.append(test_case['msg'])
        
        logging.debug("Injecting messages into twitter-source")
        self.inject_twitter_source_messages(twitter_source, injects)
        time.sleep(1.0)
        logging.debug("Consuming Messages for mongo-sink-raw")
        self.consume_mongo_sink_raw_messages(mongo_sink_raw)
        time.sleep(1.0)
        logging.debug("Consuming and processing messages for twitter-scraper")
        self.consume_twitter_scraper_messages(twitter_scraper)
        time.sleep(1.0)
        logging.debug("Consuming Messages for mongo-sink-processed")
        self.consume_mongo_sink_processed_messages(mongo_sink_processed)
        time.sleep(1.0)

    def test_process_some_tweets(self):
        messages = self.convert_content_to_msg(self.read_twitter_data())
        results = self.parseConfig()
        components = self.extract_components(results)
        try:
            self.startup_component_svcs(components)
            self.process_some_tweets(components)
        except:
            raise
        finally:
            self.shutdown_component_svcs(components)


if __name__ == '__main__':
    unittest.main()