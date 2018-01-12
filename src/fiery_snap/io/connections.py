import csv
import logging
import Queue
import json
from .message import Message
from kombu import Connection
import socket

class LogstashJsonUDPConnection(object):
    def __init__(self, name, host, port, ip_type='ipv4'):
        self.name = name
        self.host = host
        self.port = port
        self.ip_type = ip_type
        self.socket = None
        self.socket = self.get_socket()

    def get_socket(self):
        if self.socket is None and self.ip_type == 'ipv6':
            self.socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        elif self.socket is None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return self.socket

    def sendto(self, data):
        _data = data
        sent = 0
        remaining = len(data)
        if self.socket is None:
            raise Exception("Invalid Socket")

        addr = (self.host, self.port)

        while remaining > 0:
            sent = self.socket.sendto(_data, addr)
            remaining += -sent
            _data = _data[sent:]
            if sent == 0:
                break

        return len(data) - remaining

class PubSubConnection(object):

    def get_naked_in_connection(self):
        raise Exception("Not implemented")

    def get_naked_out_connection(self):
        raise Exception("Not implemented")

    @classmethod
    def get_pubsub_from_str(cls, name, uri=None, channel=None,
                                queue_name=None, iobase=None,
                                **kargs):
        # print ("Creating PubSub connection: %s %s::%s"%(name, uri, queue_name))
        uri = '' if uri is None else uri
        if uri.find('redis://') == 0 or \
           uri.find('amqp://') == 0:
           return KombuPubSubConnection(name, uri,
                                        queue_name=queue_name,
                                        iobase=iobase)
        elif uri.find('slack') == 0:
            return SlackPubSubConnection(name, uri, iobase=iobase, **kargs)
        elif uri.find('file') == 0:
            return FilePubSubConnection(name, uri, iobase=iobase, **kargs)
        elif len(uri) == 0 and iobase is not None:
            return LocalPubSubConnection(name, '', iobase=iobase)

        raise Exception("Unable to create the appropriate PubSub connection object")

    def insert(self, msg):
        return self.inserts([msg, ])

    def inserts(cls, msgs):
        raise Exception("Not implemented")

    def consume(self, cnt=1):
        raise Exception("Not implemented yet, get to work slacker")

    def consume_all(self):
        return self.consume(cnt=-1)

    def clear(self):
        raise Exception("Not implemented yet, get to work slacker")


class LocalPubSubConnection(PubSubConnection):
    def __init__(self, name, uri, iobase=None):
        self.name = 'local'
        self._iobase = iobase
        if iobase is None:
            self.in_channel = []
            self.out_channel = []
        else:
            self.in_channel = iobase.in_channel
            self.out_channel = iobase.out_channel

    def reset(self):
        del self.in_channel[:]
        del self.out_channel[:]

    def is_empty(self):
        return len(self.in_channel) == 0 and len(self.out_channel) == 0

    def inserts(self, msgs):
        logging.debug("Inserting %d msgs into the %s queue" % (len(msgs), self.name))
        for msg in msgs:
            self.out_channel.append(msg)

    def consume(self, cnt=0):
        # FIXME: what do I really want to do here
        # do i want to return messages in the in_channel, which might return
        # duplicates, because I think the in_channel holds all the messages
        # received by the other queues
        # /me(dso) thinks I made a mistake when I design this messaging approach
        return []

class KombuPubSubConnection(PubSubConnection):
    def __init__(self, name, uri, queue_name="default", iobase=None):
        self.name = name
        self.queue_name = queue_name
        self.uri = uri

    def insert(self, msg):
        self.inserts([msg, ])

    def inserts(self, msgs):
        logging.debug("Inserting %d msgs into the %s queue on %s" % (len(msgs), self.name, self.uri))
        with Connection(self.uri, connect_timeout=.5) as conn:
            logging.debug("Connected....")
            q = conn.SimpleQueue(self.queue_name)
            for msg in msgs:
                if isinstance(msg, dict):
                    q.put(msg)
                else:
                    q.put(msg.as_json())
            logging.debug("and queued messages")
            q.close()

    def consume(self, cnt=1):
        n = "all" if cnt < 1 else str(cnt)
        logging.debug("Consuming %s msgs from the %s queue" % (n, self.name))
        # trickery to consume entire queue
        # if the cnt is not a valid count we
        # consume all message by not incrementing
        # rd and exiting the loop before all messages
        # are read.
        consume_all = False if cnt > 0 else True
        cnt = 1 if cnt < 1 else cnt
        rd = 0
        msgs = []
        with Connection(self.uri) as conn:
            q = conn.SimpleQueue(self.queue_name)
            while rd < cnt:
                message = None
                if not consume_all:
                    rd += 1
                try:
                    message = q.get(block=False)
                    logging.debug("Got a message from the %s queue" % (self.name))
                except Queue.Empty:
                    logging.debug("Got no messages from the %s queue" % (self.name))
                    message = None
                except Exception as e:
                    logging.error("Error in KombuPubSubConnection: %s"%(str(e)))
                    raise
                if message is None:
                    break
                message.ack()

                m = message.payload
                try:
                    json_data = m
                    if not isinstance(json_data, dict):
                        json_data = json.loads(json_data)
                    m = Message(json_data)
                except:
                    logging.debug("Failed to load the %s message as a msg, using raw data" % (self.name))

                msgs.append(m)
        logging.debug("Retrieved %d messages from the %s queue" % (len(msgs), self.name))
        return msgs

    def reset(self):
        logging.debug("Attempting kombu reset: %s" % self.uri)
        conn = Connection(self.uri, connect_timeout=.5)
        try:
            logging.debug("Connected ... creating queue: %s" % self.queue_name)
            q = conn.SimpleQueue(self.queue_name)
            while True:
                logging.debug("Attempting to read msg from queue ...")
                m = q.get(block=False)
                m.ack()

        except Queue.Empty:
            pass
        logging.debug("Done with purge ...")

    def is_empty(self):
        try:
            q = Connection(self.uri).SimpleQueue(self.queue_name)
            message = q.get(block=False)
            if message is not None:
                message.requeue()
                return False
            return False
        except Queue.Empty:
            return True



class SlackPubSubConnection(PubSubConnection):
    def __init__(self, name, uri, channel="default", iobase=None):
        self.name = name
        self.queue_name = queue_name
        self.uri = uri
        self.out_channel = None
        # print "Created kombu queue: %s %s::%s"%(self.name, self.uri, self.queue_name)

    @property
    def out_channel(self):
        if self.out_channel is None:
            self.out_channel = Connection(self.uri)
        return self.out_channel

    def insert(self, msg):
        logging.debug("Inserting %d msgs into the %s queue" % (len(msgs), self.name))
        with self.out_channel as conn:
            # print ("Publishing msg in %s in %s::%s" % (msg[:30], self.uri, self.queue_name))
            q = conn.SimpleQueue(self.queue_name)
            q.put(msg.toJSON())
            q.close()

    def inserts(self, msgs):
        with self.out_channel as conn:
            q = conn.SimpleQueue(self.queue_name)
            for msg in msgs:
                q.put(msg)
            q.close()
        self.out_channel.out_channel.append(msg)



class FilePubSubConnection(PubSubConnection):
    ALLOWED_FILE_TYPES = ['', 'csv' ]
    def __init__(self, name, uri, channel="default", mode='ab',
                 iobase=None):
        self.name = name
        self.queue_name = queue_name
        self.uri = uri

        if uri.find("file://") == -1:
            raise Exception("Not a valid file uri")
        self.filename = uri.split("file://")[1].strip()
        self.file_type = '' if uri.find("+file://") == -1 \
                         else uri.split("+file://")[0].strip()
        self.mode = mode
        self.inserted_header = False
        self.header = []
        self.out_channel = None
        self._file = None

    @property
    def out_channel(self):
        if self.out_channel is None:
            self._file = open(self.filename, self.mode)
            self.out_channel = csv.writer(self._file)
        return self.out_channel

    def inserts(self, msgs):
        logging.debug("Inserting %d msgs into the %s queue" % (len(msgs), self.name))
        if len(self.header) == 0 and self.mode[0] != 'a':
            self.header = sorted(msg.keys())
            self.out_channel.writerow(self.header)
        # build a line
        rows = []
        for msg in msgs:
            for k in self.header:
                row.append(repr(msg[k]))

        self.out_channel.writerows(rows)
        self._file.flush()
