from fiery_snap.impl.util.page import TestPage, JsonUploadPage
from fiery_snap.impl.util.service import GenericService
from fiery_snap.utils import random_str, generate_obj_name
from .connections import PubSubConnection, KombuPubSubConnection
import logging
import threading
import time

class IOBase(object):
    SLEEP = 15 * 60
    # REQUIRED_CONFIG_PARAMS = []
    # OPTIONAL_CONFIG_PARAMS = []
    MISSING_KEY = "Missing required configuration parameter: %s"
    MISSING_VALUES = "Missing required configuration parameters"

    @classmethod
    def extract_required_keys(cls, config_dict):
        results = {}

        if len(config_dict) == 0 and len(cls.REQUIRED_CONFIG_PARAMS) > 0:
            raise Exception(cls.MISSING_VALUES)

        if len(config_dict) > 0 and len(cls.REQUIRED_CONFIG_PARAMS) > 0:
            for k in cls.REQUIRED_CONFIG_PARAMS:
                results[k] = config_dict.get(k)

        if len(config_dict) > 0 and len(cls.REQUIRED_CONFIG_PARAMS) > 0:
            for k, v in list(cls.OPTIONAL_CONFIG_PARAMS.items()):
                results[k] = config_dict.get(k, v)
        return results

    def add_incoming_message(self, msg):
        if msg is None:
            return False
        self.in_lock.acquire()
        self.in_channel.append(msg)
        self.in_lock.release()
        return True

    def pop_all_in_messages(self):
        self.in_lock.acquire()
        msgs = self.in_channel[:]
        del self.in_channel[:]
        self.in_lock.release()
        return msgs


    def _setup_pubsub(self, config_dict, what="publishers"):
        whats = self.config.get(what, {})
        add_local = False
        if len(whats) == 0:
            add_local = True

        for name, w in list(whats.items()):
            name = w.get('name', name)
            qname = w.get('queue_name', None)
            uri = w.get('uri', "")
            p = PubSubConnection.get_pubsub_from_str(name, uri=uri,
                                                         queue_name=qname,
                                                         iobase=self)
            # self.subscribers[name] = p
            if what == 'subscribers':
                self.add_subscriber(p)
            else:
                self.add_publisher(p)

        if add_local:
            p = PubSubConnection.get_pubsub_from_str("local", iobase=self)
            if what == 'subscribers':
                self.add_subscriber(p)
            else:
                self.add_publisher(p)

    def __init__(self, config_dict, pages=[JsonUploadPage, TestPage]):
        self.config = {}
        self.metas = {}
        self.sleep = config_dict.get("sleep", self.SLEEP)
        self.subscribers = {}
        self.publishers = {}
        self.in_channel = []
        self.in_lock = threading.Lock()
        self.out_channel = []
        self.out_lock = threading.Lock()

        svc_dict = config_dict.get('service', None)
        if svc_dict is None:
            raise Exception("Services section is required")

        svc_kargs = {'back_end': self,
                     'page_objs': pages}
        for k in ['listening_address',
                  'listening_port',
                  'name']:
            svc_kargs[k] = svc_dict.get(k, None)

        self.svc = GenericService(**svc_kargs)
        for k in self.REQUIRED_CONFIG_PARAMS:
            if k not in config_dict:
                raise Exception(self.MISSING_KEY % k)

            self.config[k] = config_dict.get(k, None)

        my_config_dict = dict(self.OPTIONAL_CONFIG_PARAMS)

        for k,v in list(my_config_dict.items()):
            self.config[k] = config_dict.get(k, v)

        self._setup_pubsub(self.config, 'publishers')
        self._setup_pubsub(self.config, 'subscribers')
        self.KEEP_RUNNING = False
        self.t = None

    def set_svc_handler(self, instance):
        logging.debug("Changing backend from %s to %s" % (self.svc.back_end, instance))
        self.svc.back_end = instance

    def mutate_meta_modifiers(self, lamda_or_fn, key, value):
        '''
            lambda_or_fn: a function or lambda object that must accept the
                          meta dictionary, the key, and value
            key: value to operate on
            value: something of value

            @rvalue should be a dictionary that replaces the metas
        '''
        self.metas = lamda_or_fn(self.metas, key, value)
        return self.metas

    def update(self, *args, **kargs):
        pass

    @classmethod
    def parse_config(cls, config_file):
        raise Exception("Not implemented yet, get to work slacker")

    @classmethod
    def setup(self):
        raise Exception("Not implemented yet, get to work slacker")

    def format_message(self, message):
        return repr(message)

    def has_input(self):
        return len(in_channel) > 0

    def process_input(self, results):
        while self.has_input():
            # fifo
            r = self.in_channel.pop(0)
            self.write_results(r)

    def recv_message(self, message):
        self.in_channel.append(message)

    def send_message(self, named_destinations=[]):
        raise Exception("Not implemented yet, get to work slacker")

    def read_message(self, named_sources=[]):
        return self.recv_message(self, named_sources=named_sources)

    def write_messasge(self, named_destinations=[]):
        return self.send_message(named_destinations=named_destinations)

    def main(self, *args, **kargs):
        raise Exception("Not implemented yet, get to work slacker")

    def insert(self):
        raise Exception("Not implemented yet, get to work slacker")

    def consume(self, cnt=1):
        # msgs = []
        # for name, publisher in self.publishers.items():
        #     json_msgs = publisher.consume(cnt=cnt)
        #     for msg_dict in json_msgs:
        #         m = Message(msg_dict)
        #         msgs.append(m)

        #         if cnt - len(msgs) <= 0 and cnt > 0:
        #             cnt = 0
        #             break
        # return msgs
        raise Exception("Not implemented")

    def random_name(self, prepend='', append='', name_len=5):
        return "%s.%s.%s" % (prepend, random_str(length=name_len), append)

    def add_publisher(self, obj):
        on = getattr(obj, 'name') if hasattr(obj, 'name') \
                else generate_obj_name(obj)
        self.publishers[on] = obj

    def add_subscriber(self, obj):
        on = getattr(obj, 'name') if hasattr(obj, 'name') \
                else generate_obj_name(obj)
        self.subscribers[on] = obj

    def publish(self, msg, subscribers=[], exclude=[]):
        if len(subscribers) == 0:
            for name, subscriber in list(self.subscribers.items()):
                if name == 'local' or name in exclude:
                    continue
                subscriber.insert(msg)
        else:
            for name in subscribers:
                subscriber = self.subscribers.get(name, None)
                if name is None or name == 'local':
                    continue
                subscriber.insert(msg)


    def publish_all_msgs(self, msgs):
        for name, subscriber in list(self.subscribers.items()):
            if name == 'local':
                continue
            subscriber.inserts(msgs)

    def consume_and_publish(self):
        raise Exception("Not implemented")

    def _run(self):
        self.logger = logging.getLogger('{}:{}'.format(__name__, self.name))
        while True:
            self.logger.info('_run(): Executing')
            message = self.main()
            self.publish(message)
            if self.outputs:
                # Do something
                pass
            gevent.sleep(self.sleep)

    @classmethod
    def undefang(cls, domain):
        return domain.replace('[', '').replace(']', '')

    @classmethod
    def defang(cls, domain):
        return domain.replace('.', '[.]')

    @classmethod
    def class_map_key(cls):
        return cls.KEY.lower()

    @classmethod
    def parse(cls, config_dict):
        return cls(config_dict)

    def stop_periodic_consume(self):
        self.KEEP_RUNNING = False
        if self.t is None or not self.t.isAlive():
            return True
        self.t.join()

    def periodic_consume(self, sleep_time=30.0):
        sleep_time = self.config.get('sleep_time', sleep_time)
        logging.debug("Starting service with sleep time of %ds"%sleep_time)
        if self.t is None or not self.t.isAlive():
            if not self.svc.is_alive():
                self.svc.start()
            self.KEEP_RUNNING = True
            self.t = threading.Thread(target=self._periodic_consume, args=(sleep_time,))
            self.t.start()
            return True
        return False

    def _periodic_consume(self, sleep_time):
        if not self.svc.is_alive():
            self.svc.start()

        while self.KEEP_RUNNING:
            self.consume_and_publish()
            time.sleep(sleep_time)

    def reset_all(self):
        for n, pubsub in list(self.publishers.items()):
            try:
                pubsub.reset()
            except:
                logging.error("Failed to reset: %s:%s"%(n, type(pubsub)))

        for n, pubsub in list(self.subscribers.items()):
            try:
                pubsub.reset()
            except:
                logging.error("Failed to reset: %s:%s"%(n, type(pubsub)))

    def handle(self, path, data):
        logging.info("handling request (%s)" % (path))
        if path == TestPage.NAME:
            {'msg': 'success'}
        return {'error': 'unable to handle message type: %s' % path}

    @classmethod
    def key(cls):
        return cls.class_map_key()

    def is_empty(self):
        results = {}
        for n, pubsub in list(self.publishers.items()):
            try:
                results[n] = pubsub.is_empty()
            except:
                logging.error("Failed to reset: %s:%s"%(n, type(pubsub)))
                results[n] = None

        for n, pubsub in list(self.subscribers.items()):
            try:
                results[n] = pubsub.is_empty()
            except:
                logging.error("Failed to reset: %s:%s"%(n, type(pubsub)))
                results[n] = None
        return results
