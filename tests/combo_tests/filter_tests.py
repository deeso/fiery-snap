from unittest import TestCase
from fiery_snap.parse import ConfigParser, INPUTS, CLASS_MAPS
import toml
import threading
import time
import logging
import sys

logging.getLogger().setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s - %(name)s] %(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)
EXAMPLE_TOML = '''
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
    # rig-e = '^rig-e.*'
    # rig-v = '^rig-v.*'
    # rigek = '^rigek.*'
    rulan = '^rulan.*'
    seamless = '^seamless.*'
    sundown = '^sundown.*'
    terror = '^terror.*'
    teslacrypt = '^teslacrypt.*'



[inputs]
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
               'Zerophage1337', 'baberpervez2', 'repmovsb', ''  ]

    [inputs.twitter01.subscribers.redis_queue]
        name = 'redis_queue'
        queue_name = "unfiltered_feeds"
        uri = "redis://192.168.122.10:6379"
        limit = 10 # adjustable limit 10 posts per request for content
'''



class TestMongoTwitter(TestCase):

    def setup(self):
        pass

    def populate_queue(self):
        pass

    def consume_queue(self):
        pass

    def check_results(self):
        pass


KEEP_RUNNING = False
TW_G = None
def run_twitter01(inputs_dict):
    global KEEP_RUNNING, TW_G
    KEEP_RUNNING = True
    inputs = ConfigParser.parse_inputs(inputs_dict, CLASS_MAPS)
    twitter01 = inputs.get('twitter01')
    TW_G = twitter01
    while KEEP_RUNNING:
        try:
            posts = twitter01.consume_and_publish()
            time.sleep(5.0)
            if posts is None:
                continue
            num_msgs = sum([len(i) for i in posts.values()])
            print ("Found %d posts for %d handle" % (num_msgs, len(posts)))
        except KeyboardInterrupt:
            KEEP_RUNNING = False
            print ("Ending execution")
            break


# config = "my_config.toml"
# # if len(sys.argv) > 1:
# #     config = sys.argv[1]

# toml_dict = toml.load(open(config))
# inputs_dict = toml_dict.get(INPUTS)
# inputs = ConfigParser.parse_inputs(inputs_dict, CLASS_MAPS)
# twitter01 = inputs.get('twitter01')

if __name__ == "__main__":
    config = "my_config.toml"
    if len(sys.argv) > 1:
        config = sys.argv[1]

    toml_dict = toml.load(open(config))
    inputs_dict = toml_dict.get(INPUTS)
    inputs = ConfigParser.parse_inputs(inputs_dict, CLASS_MAPS)
    twitter01 = inputs.get('twitter01')

    twitter01.periodic_consume(2.0)


