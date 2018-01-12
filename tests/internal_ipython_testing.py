import toml
from twittersource_tests.twitter_data import get_json_data, TWITTER_DATA
from twittersource_tests.twittersource_test import *
from fiery_snap.parse import ConfigParser, INPUTS, CLASS_MAPS

data = get_json_data()
toml_dict = toml.loads(BASIC_TS_CONFIG)

KEYS = ('keys', [])
MINE_NOT_YOURS = 'internal_keys.toml'


config = toml.load(open("internal_keys.toml"))
keys = config.get(KEYS[0], KEYS[1])
for k in keys:
    classname, attr, value = config.get(k)
    # this works because imported all names/class
    # for the test modules into the local space
    klass = locals()[classname]
    #  Set the static attributes in the class
    setattr(klass, attr, value)

TS_NAME = 'twitter01'
INPUTS = 'inputs'
toml_dict = toml.loads(BASIC_TS_CONFIG)
#  thoughtful protection of secrets
toml_dict[INPUTS][TS_NAME]['consumer_key'] = TwitterSourceConfig.consumer_key
toml_dict[INPUTS][TS_NAME]['consumer_secret'] = TwitterSourceConfig.consumer_secret
toml_dict[INPUTS][TS_NAME]['access_token'] = TwitterSourceConfig.access_token
toml_dict[INPUTS][TS_NAME]['access_token_secret'] = TwitterSourceConfig.access_token_secret

inputs_dict = toml_dict.get(INPUTS)
inputs = ConfigParser.parse_inputs(inputs_dict, CLASS_MAPS)

twitter01 = inputs[TS_NAME]
started = twitter01.svc.start()

