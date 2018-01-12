import requests
import unittest
import toml
import time
from .twitter_data import get_json_data as get_twtr_msgs, TWITTER_DATA
from fiery_snap.parse import ConfigParser, INPUTS, CLASS_MAPS
from fiery_snap.impl.util.page import TestPage, JsonUploadPage

# these will be set by main
class TwitterSourceConfig(object):
    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_token_secret = ''


BASIC_TS_CONFIG = '''
[inputs.twitter01]
name = 'twitter01'
type = 'simple-in-twitter'
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

handles = ['nao_sec', 'malwrhunterteam', 'Racco42', 'SecGuru', 'illegalFawn', 'dvk01uk', 'Ring0x0', 
           'James_inthe_box', 'azsxdvfbg', 'VK_Intel', 'Stormshield', 'Antelox', 'DynamicAnalysis',
           'thlnk3r', 'BroadAnalysis', 'jeromesegura', 'w1mp1k1ng', 'malc0de', 'EKwatcher', 'Kafeine',
           'Zerophage1337', 'baberpervez2']

[inputs.twitter01.service]
    name = 'twitter01_service'
    listening_port = 17878
    listening_address = '0.0.0.0'
[inputs.twitter01.subscribers.redis_queue]
    name = 'unfiltered_twitter_feed'
    queue_name = "unfiltered_twitter_feed"
    uri = "redis://0.0.0.0:6379"
    limit = 10000 # adjustable limit 10 posts per request for content
'''

TS_NAME = 'twitter01'
INPUTS = 'inputs'
class TwitterSourceTest(unittest.TestCase):
    
    def parseBasicConfig(self):
        toml_dict = toml.loads(BASIC_TS_CONFIG)
        #  thoughtful protection of secrets

        toml_dict[INPUTS][TS_NAME]['consumer_key'] = TwitterSourceConfig.consumer_key
        toml_dict[INPUTS][TS_NAME]['consumer_secret'] = TwitterSourceConfig.consumer_secret
        toml_dict[INPUTS][TS_NAME]['access_token'] = TwitterSourceConfig.access_token
        toml_dict[INPUTS][TS_NAME]['access_token_secret'] = TwitterSourceConfig.access_token_secret
        inputs_dict = toml_dict.get(INPUTS)
        inputs = ConfigParser.parse_inputs(inputs_dict, CLASS_MAPS)
        return inputs

    def test_insertMessagesTest(self):
        twitter_msgs = get_twtr_msgs()
        self.assertTrue(len(TWITTER_DATA) == len(twitter_msgs))
        inputs = self.parseBasicConfig()
        twitter01 = inputs[TS_NAME]
        started = twitter01.svc.start()
        self.assertTrue(started)
        self.assertTrue(twitter01.svc.is_alive())
        time.sleep(2.0)
        
        twitter01.reset_all()
        location = twitter01.svc.get_base_url(JsonUploadPage.NAME)
        msg_batch_1 = twitter_msgs[:100] 
        msg_batch_2 = twitter_msgs[100:198]
        msg_batch_2_msg = twitter_msgs[199]
        r = requests.post(location, json={'msgs':msg_batch_1, 
                                          'target': twitter01.svc.name})
        self.assertTrue(r.status_code == 200)

        r = requests.post(location, json={'msgs': msg_batch_2,
                                      'msg': msg_batch_2_msg,
                                      'target': twitter01.svc.name})
        self.assertTrue(r.status_code == 200)

        qs_state = twitter01.is_empty()
        self.assertFalse(qs_state['unfiltered_twitter_feed'])
        twitter01.reset_all()
        qs_state = twitter01.is_empty()
        self.assertTrue(qs_state['unfiltered_twitter_feed'])
        twitter01.svc.stop()
        self.assertFalse(twitter01.svc.is_alive())

    def test_BasicParse(self):
        # test the assertions here
        inputs = self.parseBasicConfig()
        twitter01 = inputs[TS_NAME]
        self.assertTrue(twitter01.config.get('consumer_key') == TwitterSourceConfig.consumer_key)
        started = twitter01.svc.start()
        twitter01.reset_all()
        self.assertTrue(started)
        self.assertTrue(twitter01.svc.is_alive())
        time.sleep(2.0)
        twitter01.svc.stop()
        self.assertFalse(twitter01.svc.is_alive())


if __name__ == '__main__':
    unittest.main()
