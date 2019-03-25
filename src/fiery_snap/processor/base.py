from fiery_snap.impl.util.service import GenericService
from fiery_snap.impl.util.page import Page, AddHandlesPage, \
               ListHandlesPage, RemoveHandlesPage, ConsumePage, \
               JsonUploadPage, TestPage

from fiery_snap.io.connections import PubSubConnection
import threading
import logging
import json
import time

class BaseOperation(object):
    UNABLE_TO_CREATE = "Unable to create the %s code object from: %s "
    UNABLE_TO_FIND = "Unable to find the code object for: %s "


    def __init__(self, name, raw_code, fn_obj, metas):
        self.name = name
        self.raw_code = raw_code
        self.fn = fn_obj
        self.metas = metas

    @classmethod
    def parse(cls, config_dict):
        name = config_dict.get('name', None)
        metas = config_dict.get('metas', {}).copy()
        raw_code = config_dict.get('code', None)
        if raw_code is None:
            raise Exception("Mising extraction code parameters")

        co = None
        fn_obj = None
        try:
            co = compile(raw_code, '<string>', 'exec')
        except Exception as e:
            print(raw_code)
            raise Exception(cls.UNABLE_TO_CREATE % (name, str(e)))

        if co is not None:
            eval(co)

        fn_obj = globals().get(name, None)
        if fn_obj is None:
            fn_obj = locals().get(name, None)

        if fn_obj is None:
            raise Exception(cls.UNABLE_TO_FIND % name)
        return cls(name, raw_code, fn_obj, metas)

    def execute(self, state, message):
        self.fn(self.metas, state, message)

    @classmethod
    def class_map_key(cls):
        return cls.KEY.lower()


class Processor(BaseOperation):
    KEY = 'processor'

    def __init__(self, name, raw_code, fn_obj, metas):
        BaseOperation.__init__(self, name, raw_code, fn_obj, metas)


class Extractor(BaseOperation):
    KEY = 'extractor'

    def __init__(self, name, raw_code, fn_obj, metas):
        BaseOperation.__init__(self, name, raw_code, fn_obj, metas)


class Transform(BaseOperation):
    KEY = 'transform'

    def __init__(self, name, raw_code, fn_obj, metas):
        BaseOperation.__init__(self, name, raw_code, fn_obj, metas)


class BaseProcessor(object):
    SLEEP = 15 * 60
    MISSING_KEY = "Missing required configuration parameter: %s"
    MISSING_VALUES = "Missing required configuration parameters"

    def __init__(self, name, transforms={},
                 extractors={}, processors={},
                 subscriber_polling=1.0, message_count=300, **kargs):
        self.message_count = message_count
        self.in_lock = threading.Lock()
        self.out_lock = threading.Lock()
        self.in_channel = []
        self.out_channel = []
        self.name = name
        self.transforms = transforms
        self.extractors = extractors
        self.subscriber_polling = subscriber_polling
        self.sleep_time = kargs.get('sleep_time', 60.0)


        config = {'publishers': kargs.get('publishers', {}),
                  'subscribers': kargs.get('subscribers', {})
                  }

        self.publishers = {}
        self.subscribers = {}
        self._setup_pubsub(config, 'publishers')
        self._setup_pubsub(config, 'subscribers')

        self.consume_and_process = True
        self.subscriber_thread = None
        self.t = None
        self.KEEP_RUNNING = False

        svc_dict = kargs.get('service', {})
        svc_kargs = {'back_end': self,
                     'page_objs': kargs.get('pages', [])}
        for k in ['listening_address',
                  'listening_port',
                  'name']:
            svc_kargs[k] = svc_dict.get(k, None)

        self.svc = GenericService(**svc_kargs)



    def add_publisher(self, pub_conn):
        self.publishers[pub_conn.name] = pub_conn

    def add_subscriber(self, sub_conn):
        self.subscribers[sub_conn.name] = sub_conn

    def stop_consuming(self):
        self.consume_and_process = False
        # probably should kill the thread
        # but that turns out to be pretty hard sometimes iirc

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

    #def run_consumer_thread(self):
    #    if not self.consume_and_process and self.subscriber_thread is not None:
    #        try:
    #            self.subscriber_thread.cancel()
    #        except:
    #            pass
    #        self.subscriber_thread = None
    #    else:
    #        try:
    #            # FIXME is this necessary?
    #            self.subscriber_thread.cancel()
    #        except:
    #            pass

    #        self.subscriber_thread = Timer(self.subscriber_polling, self.consume_process_publish)
    #        self.subscriber_thread.start()
    #    return self.subscriber_thread

    def consume_process_publish(self):
        inmsgs = self.consume()
        pmsgs = self.process(inmsgs)
        return self.publish_results(pmsgs)

    def consume_and_publish(self):
        return self.consume_process_publish()

    def add_incoming_message(self, msg):
        if msg is None:
            return False
        self.in_lock.acquire()
        self.in_channel.append(msg)
        self.in_lock.release()
        return True

    def add_outgoing_message(self, msg):
        if msg is None:
            return False
        self.out_lock.acquire()
        self.out_channel.append(msg)
        self.out_lock.release()
        return True

    def pop_all_in_messages(self):
        msgs = []
        self.in_lock.acquire()
        while len(self.in_channel) > 0:
            m = self.in_channel.pop(0)
            msgs.append(m)
        self.in_lock.release()
        return msgs

    def pop_all_out_messages(self):
        _msgs = []
        msgs = []
        self.out_lock.acquire()
        msgs = self.out_channel[:]
        del self.out_channel[:]
        self.out_lock.release()
        return msgs

    def consume(self, cnt=1):
        cnt = self.message_count
        messages = []
        #  conn == ..io.connection.Connection
        for name, conn in list(self.publishers.items()):
            pos = 0
            if name == 'local':
                continue
            msgs = conn.consume(cnt=cnt)
            if msgs is None or len(msgs) == 0:
                continue
            # logging.debug("Retrieved %d messages from the %s queue" % (len(msgs), name))
            logging.debug("Adding messages to the internal queue" )
            for m in msgs:
                messages.append(m)
                if not self.add_incoming_message(m):
                    logging.debug("Failed to add message to queue" )
        return messages

    def publish_results(self, msgs):
        #msgs = self.pop_all_out_messages()
        for m in msgs:
            self.publish(m)
        return {"out_channel":msgs}

    def process(self, msgs):
        results = []
        omsgs = []
        #msgs = self.pop_all_in_messages()
        for m in msgs:
            try:
                omsgs.append(json.loads(m))
            except:
                omsgs.append(m)
        for m in omsgs:
            nmessage = self.process_message(m)
            self.add_outgoing_message(nmessage)
            results.append(nmessage)
        return results

    def _setup_pubsub(self, config_dict, what="publishers"):
        whats = config_dict.get(what, {})
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
            p = PubSubConnection.get_pubsub_from_str("local", uri="", iobase=self)
            if what == 'subscribers':
                self.add_subscriber(p)
            else:
                self.add_publisher(p)

    def insert(self, message):
        for name, pub in list(self.publishers.items()):
            pub.insert(message)


    def setup(self):
        raise Exception("Not implemented yet, get to work slacker")

    @classmethod
    def parse(cls, config_file, **kargs):
        raise Exception("Not implemented yet, get to work slacker")

    def main(self, *args, **kargs):
        raise Exception("Not implemented yet, get to work slacker")

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

    @classmethod
    def class_map_key(cls):
        return cls.KEY.lower()

    def stop_periodic_consume(self):
        self.KEEP_RUNNING = False
        if self.t is None or not self.t.isAlive():
            return True
        self.t.join()

    def periodic_consume(self, sleep_time=None):
        sleep_time = self.sleep_time
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
            self.consume_process_publish()
            time.sleep(sleep_time)

    def handle(self, path, data):
        logging.debug("handling request (%s): %s" % (path, data))
        if path == TestPage.NAME:
            {'msg': 'success'}
        return {'error': 'unable to handle message type: %s' % path}
