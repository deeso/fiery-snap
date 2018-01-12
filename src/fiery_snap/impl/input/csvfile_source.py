import os
import json
import csv
from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message


class CSVFileImpl(object):
    TWT_FMT = 'https://twitter.com/{}/status/{}'
    URL_LOC = 'https://t.co'
    REQUIRED_CONFIG_PARAMS = ['filename', ]
    OPTIONAL_CONFIG_PARAMS = [['sleep_period', 60],
                              ['remove', False],
                              ['quotechar', '"'],
                              ['delimiter', ','],
                              ['content', None]]

    def __init__(self, **kargs):
        self.filename = None
        self.remove = False
        self.sleep_period = None
        self.quotechar = '"'
        self.delimiter = ','

        for k in self.REQUIRED_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k))

        for k, v in self.OPTIONAL_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k, v))

    def consume_source(self):
        def extract_content(row):
            if self.content is None or self.content not in row:
                return json.dumps(row)
            return row[self.content]

        msgs = []
        with open(self.filename, 'rb') as csvfile:
            datareader = csv.DictReader(csvfile, delimiter=self.delimiter,
                                        quotechar=self.quotechar)
            for row in datareader:
                js = {}
                content = extract_content(row)
                js['meta'] = row
                js['content'] = content
                msgs.append(Message(js))
        return msgs

    def test_source(self):
        try:
            os.stat(self.filename)
        except:
            return False
        return True


class CSVFile(IOBase):
    KEY = 'simple-in-csvfile'
    REQUIRED_CONFIG_PARAMS = ['filename', ]
    OPTIONAL_CONFIG_PARAMS = [['sleep_period', 60],
                              ['remove', False],
                              ['quotechar', '"'],
                              ['delimiter', ','],
                              ['content', None]]

    def __init__(self, config_dict):
        IOBase.__init__(self, config_dict)
        self.output_queue = []
        rs = self.random_name(prepend='csvfile-in')
        self.name = self.config.get('name', rs)

    def consume_source(self):
        tc = self.new_client()
        posts = tc.consume_source()
        # update config with the last id
        self.config['last_id'] = tc.last_id
        self.publish_results({'source': self.name, 'posts': posts})

    def publish_results(self, results):
        pass

    def new_client(self):
        return CSVFileImpl(**self.config)
