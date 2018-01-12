from .twitterscraper_test_messages import *

V = 'v'
C = 'c'

TEST_CASES_ID_MAPPING = {
    1: "NO_HT_JSON_CASE",
    2: "LOCKY_HT",
    3: "LOCKY_HT_IPS_DOMAINS",
}

TEST_CASES = {
    "NO_HT_JSON_CASE": {
        'msg': NO_HT_JSON, 
        'results': None,
        # [
        #     (C,  {'entities': [(V, {'ips': []}), 
        #                        (V,{'hashes': []}), 
        #                        (V, {'urls': []}),
        #                        (V, {'emails': []}),
        #                        ]}),
        #     (V, {'linked_content': {}}),
        # ],
    },

    "LOCKY_HT": {
        'msg': LOCKY_HT, 
        'results':[
            (V, {'tags': ['locky',]}),
        ],
    },

    "LOCKY_HT_IPS_DOMAINS": {
        'msg': LOCKY_HT_IPS_DOMAINS, 
        'results': [
            (C,  {'defanged_entities': [
                    (V, {'ips': [u'127.0.0.1',]}),
                    (V, {'domains': ['www.test123.com',]}),
                    (V, {'urls': [ 'https://www.test456.com']}),
                    (V, {'emails': ['phser@test123.com',]}),
                    ]}),
            (C,  {'entities': [
                    (V, {'ips': [u'127.0.2.1',]}), 
                    (V, {'hashes': []}), 
                    (V, {'domains': []}),
                    (V, {'urls': ['https://www.test789.com', ]}),
                    (V, {'emails': []}),
                    ]}),
        ]
    },

}