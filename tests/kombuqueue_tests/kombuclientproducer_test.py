from unittest import TestCase
from kombu import Connection
import time
import toml
import requests
from fiery_snap.impl.input.kombu_source import KombuClientProducer
from fiery_snap.impl.util.page import TestPage, JsonUploadPage
from fiery_snap.io.connections import KombuPubSubConnection

REQ = {'pong':'test', 'target': 'kombu_service'}

KOMBU_URI = 'redis://0.0.0.0:6379'
KOMBU_QUENAME = 'internal'
EXAMPLE_TOML = '''
[inputs.kombu_test]
name = 'kombu_test'
listen = true
type = 'kombu-client-producer'
uri = '{0}'
queue_name = '{1}'

[inputs.kombu_test.service]
    name = 'kombu_service'
    listening_port = 17878
    listening_address = '0.0.0.0'

[inputs.kombu_test.subscribers.insert_queue]
    name = 'insert_queue'
    queue_name = "unfiltered_test_feed"
    uri = "redis://10.18.120.11:6379"
'''.format(KOMBU_URI, KOMBU_QUENAME)

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


class TestKombuSource(TestCase):

    def testParse(self):
        config_dict = toml.loads(EXAMPLE_TOML)
        # get the kombu
        producer_config = config_dict.get('inputs').get('kombu_test')
        kpc = KombuClientProducer.parse(producer_config)
        started = kpc.svc.start()
        self.assertTrue(started)
        self.assertTrue(kpc.svc.is_alive())
        time.sleep(2.0)
        kpc.svc.stop()
        self.assertFalse(kpc.svc.is_alive())


    def testJsonStart(self):

        messages = {'messages': TEST_MSGS}
        config_dict = toml.loads(EXAMPLE_TOML)
        # get the kombu
        producer_config = config_dict.get('inputs').get('kombu_test')
        kpc = KombuClientProducer.parse(producer_config)
        started = kpc.svc.start()
        self.assertTrue(started)
        self.assertTrue(kpc.svc.is_alive())
        time.sleep(2.0)
        location = kpc.svc.get_base_url(TestPage.NAME)
        r = requests.post(location, json=REQ)

        kpc.svc.stop()
        self.assertFalse(kpc.svc.is_alive())
        self.assertTrue(r.status_code == 200)





if __name__ == '__main__':
    unittest.main()
