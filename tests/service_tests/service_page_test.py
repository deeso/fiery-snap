import logging
import unittest
from unittest import TestCase
import toml
from fiery_snap.impl.util.service import GenericService
from fiery_snap.impl.util.page import Page, TestPage
import time
import web
import requests

EXAMPLE_TOML = """
name = "test_service"
listening_address = "0.0.0.0"
listening_port = 7878
"""

REQ = {'pong':'test', 'target': 'test_service'}

class MyService(GenericService):
    pass

class MockFrontend(object):
    NAME = 'mock-frontend'
    def __init__(self, name=NAME):
        self.name = name

    @classmethod
    def handle(cls, pagename, json_data):
        result = {'pagename': pagename,
                  'pong': json_data.get('pong', 'failed')}
        return result

class TestBasicService(TestCase):
    def test_parseblock(self):
        config_dict = toml.loads(EXAMPLE_TOML)
        svc_config = config_dict
        pages = [TestPage,]
        config_dict['page_objs'] = pages

        TestPage.BACKEND[MockFrontend.NAME] = MockFrontend
        svc = MyService(back_end=MockFrontend(), **config_dict)
        try:
            started = svc.start()
            self.assertTrue(started)
            self.assertTrue(svc.is_alive())
            time.sleep(2.0)
        except:
            raise
        finally:
            svc.stop()

        self.assertFalse(svc.is_alive())

    def test_getpage(self):
        config_dict = toml.loads(EXAMPLE_TOML)
        svc_config = config_dict
        pages = [TestPage,]
        config_dict['page_objs'] = pages
        
        svc = MyService(back_end=MockFrontend(), **config_dict)
        started = svc.start()
        self.assertTrue(started)
        try:
            self.assertTrue(svc.is_alive())
            time.sleep(4.0)
            location = svc.get_base_url(TestPage.NAME)
            logging.debug("Sending request to: %s" %location)
            r = requests.post(location, json=REQ)
            self.assertTrue(r.status_code == 200)
            jd = r.json()
            logging.debug("Testpage returned: %s" %r.text)
            self.assertTrue('pong' in jd and jd['pong'] == REQ['pong'])
        except:
            raise
        finally:
            svc.stop()
        self.assertFalse(svc.is_alive())

if __name__ == '__main__':
    unittest.main()
