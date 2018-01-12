import os
# from mongo_tests.mongosink_test import *
# from mongo_tests.mongofilter_test import *
# from service_tests.service_page_test import *
# from kombuqueue_tests.kombuclientproducer_test import *
# from twittersource_tests.twittersource_test import *
from twitterscraper_tests.twitterscraper_test import *
# from dataflow_tests.twittersource_mongosink_test import *
# from dataflow_tests.twittersource_mongosink_twitterscraper_test import *


import logging
import sys
import toml
import os
import argparse
import signal

logging.getLogger().setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s - %(name)s] %(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)


parser = argparse.ArgumentParser(
                      description='Unit testing for fiery snap.')

parser.add_argument('-config', type=str, default=None,
                    help='toml config for keys and such, see key.toml')


KEYS = ('keys', [])
MINE_NOT_YOURS = 'internal_keys.toml'

if __name__ == '__main__':
    args = parser.parse_args()
    if args.config is None:
        try:
            os.stat(MINE_NOT_YOURS)
            args.config = MINE_NOT_YOURS
        except:
            pass

    if args.config is not None:
        config = toml.load(open(args.config))
        keys = config.get(KEYS[0], KEYS[1])
        for k in keys:
            classname, attr, value = config.get(k)
            # this works because imported all names/class
            # for the test modules into the local space
            klass = locals().get(classname, None)
            if klass is not None:
                #  Set the static attributes in the class
                setattr(klass, attr, value)

    unittest.main()
    os.kill(os.getpid(), signal.SIGKILL) 
