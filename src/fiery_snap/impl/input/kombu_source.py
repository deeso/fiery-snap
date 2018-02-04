from fiery_snap.io.io_base import IOBase
from fiery_snap.io.message import Message
from fiery_snap.impl.util.page import JsonUploadPage, TestPage
from fiery_snap.utils import random_str
import logging


class KombuClientProducer(IOBase):
    KEY = 'kombu-client-producer'
    REQUIRED_CONFIG_PARAMS = ['name',
                              'uri',
                              'queue_name',
                              'publishers',
                              'subscribers']
    OPTIONAL_CONFIG_PARAMS = []

    def __init__(self, config_dict, **kargs):
        IOBase.__init__(self, config_dict, pages=[JsonUploadPage, TestPage])

    @classmethod
    def key(cls):
        return cls.KEY

    def consume(self):
        msgs = self.my_queue.consume(-1)
        return msgs

    def publish_msg(self, msg):
        return self.publish_all_msgs([msg, ])

    def publish_all_msgs(self, msgs):
        cnt = 0
        for posts in msgs:
            m = "Publishing msgs %d msgs" % (len(posts))
            logging.debug(m)
            for msg in posts:
                cnt += 1
                self.publish(msg)
        logging.debug("Published %d msgs to all subscribers" % (cnt))
        return cnt

    @classmethod
    def parse(cls, config_dict):
        rn = cls.key()+'-'+random_str()
        kargs = {}
        kargs['name'] = config_dict.get("name", rn)
        kargs['uri'] = config_dict.get("uri", None)
        kargs['queue_name'] = config_dict.get("queue_name", None)
        kargs['publishers'] = config_dict.get("publishers", {})
        kargs['subscribers'] = config_dict.get("subscribers", {})
        kargs['service'] = config_dict.get('service', {})
        if kargs['uri'] is None:
            raise Exception("uri are required")
        if kargs['queue_name'] is None:
            raise Exception("queue_name are required")

        return cls(kargs)

    def handle(self, path, data):
        if path == JsonUploadPage.NAME:
            msgs = data.get('messages', [])
            messages = []
            for msg in msgs:
                m = Message()
                m.msg.update(msg)
                messages.append(m)
            self.my_queue.inserts(messages)
            self.publish_all_msgs(messages)
            return {'msg': 'published %d messages in the queue'}
        return {'error': 'unable to handle message type: %s' % path}
