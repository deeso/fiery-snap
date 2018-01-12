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
from fiery_snap.parse import ConfigParser, PROCESSORS, CLASS_MAPS, TAGS, OSINT
from fiery_snap.impl.processor.twitter_scraper_processor import DEBUG_CONTENT

from fiery_snap.impl.util.page import TestPage, JsonUploadPage, ConsumePage
from fiery_snap.io.message import Message
from fiery_snap.io.connections import LogstashJsonUDPConnection

from .twitter_data import get_json_data as get_twtr_msgs, TWITTER_DATA
from .twitterscraper_test_messages_config import TEST_CASES, C, V

TP_CONFIG = '''
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
        listening_port = 19001
        listening_address = '0.0.0.0'
    [processors.twitter-scraper.publishers.ts-twitter-feed]
        name = 'ts-twitter-feed'
        queue_name = "ts-twitter-feed"
        uri = "redis://redis-queue-host:6379"

    [processors.twitter-scraper.subscribers.ms-scraped-twitter]
        name = 'ms-scraped-twitter'
        queue_name = "ms-scraped-twitter"
        uri = "redis://redis-queue-host:6379"

    [processors.twitter-scraper.subscribers.local]
        name = 'local'
        uri = ""
'''

TP_NAME = 'twitter-scraper'
INPUTS = 'inputs'
PROCESSORS = 'processors'
class TwitterScraperTest(unittest.TestCase):

    
    def convert_content_to_msg(self, twitter_msgs):
        return [Message(i) for i in twitter_msgs]
    
    def read_twitter_data(self):
        twitter_msgs = get_twtr_msgs()
        self.myassertTrue(len(TWITTER_DATA) == len(twitter_msgs))
        return twitter_msgs
        
    def parseConfig(self):
        toml_dict = toml.loads(TP_CONFIG)
        results = ConfigParser.parse_components_dict(toml_dict)
        return results
    
    def extract_components(self, results):
        components = {}
        # all the processors
        for name, item in results[PROCESSORS].items():
            components[name] = item
        return components
    
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
        if expected_results is None and actual_results is None:
            return

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
    
    def test_send_empty_json(self):
        messages = self.convert_content_to_msg(self.read_twitter_data())
        results = self.parseConfig()
        components = self.extract_components(results)
        twitter_scraper = components[TP_NAME]
        self.startup_component_svcs(components)
        time.sleep(2.0)
        
        location = twitter_scraper.svc.get_base_url(JsonUploadPage.NAME)
        r = requests.post(location, json={'msg':"test", 
                                          'target': twitter_scraper.svc.name})
        self.assertTrue(r.status_code == 200)
        self.shutdown_component_svcs(components)


    def test_process_some_tweets(self):
        messages = self.convert_content_to_msg(self.read_twitter_data())
        results = self.parseConfig()
        components = self.extract_components(results)
        twitter_scraper = components[TP_NAME]

        for name, test_case in TEST_CASES.items():
            logging.debug("Testing processing of message: %s" % (name))
            self.myassertTrue(len(set(['msg', 'results']) & set(test_case.keys())) == 2)
            msg_json = test_case['msg']
            msg = Message(msg_json)
            logging.debug("Processing msg (%s): %s" % (msg['user'], msg['content'][:50]) )
            new_msg = twitter_scraper.process_message(msg)
            actual_results = None
            if new_msg is not None:
                actual_results = new_msg.as_json()
            
            self.compare_results(test_case, actual_results)



if __name__ == '__main__':
    unittest.main()
