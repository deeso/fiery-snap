from fiery_snap.io.io_base import IOBase

import logging
import threading

class SimpleFlatfileImpl(object):
    REQUIRED_CONFIG_PARAMS = ['filename',]
    OPTIONAL_CONFIG_PARAMS = [['append', True]]

    def __init__(self, **kargs):
        self.config = {}
        for k in self.REQUIRED_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k))

        for k, v in self.OPTIONAL_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k, v))

    def write_line(self, line):
        return self.write_lines([line,])

    def write_lines(self, lines):
        mode = 'a+'
        if not self.append:
            mode = 'w'

        try:
            outfile = open(self.filename, mode)
            for line in lines:
                outfile.write(line+'\n')

            return True
        except Exception as e:
            raise e


class SimpleFlatfile(IOBase):
    REQUIRED_CONFIG_PARAMS = ['filename', 'publishers']
    OPTIONAL_CONFIG_PARAMS = [['append', True],
                              ['message_count', 10]]

    KEY = 'simple-out-flatfile'
    def __init__(self, config_file):
        IOBase.__init__(self, config_file)

    def write_results(self, results):
        if self.name not in results:
            return False
        return self.new_flat_file().post_results(results[self.name])

    def new_flat_file(self):
        return SimpleFlatfile(**self.config)

    def consume(self, cnt=1):
        cnt = self.message_count if hasattr(self, 'message_count') else cnt
        messages = []
        #  conn == ..io.connection.Connection
        for name, conn in list(self.publishers.items()):
            pos = 0
            msgs = conn.consume(cnt=cnt)
            if msgs is None or len(msgs) == 0:
                continue
            # logging.debug("Retrieved %d messages from the %s queue" % (len(msgs), name))
            logging.debug("Adding messages to the internal queue" )
            for m in msgs:
                if not self.add_incoming_message(m):
                    logging.debug("Failed to add message to queue" )


    def consume_and_publish(self):
        self.consume()
        all_msgs = self.pop_all_in_messages()
        lines = [i.msg['simple_message'] for i in all_msgs if 'simple_message' in i.msg]
        self.write_lines(lines)

    def write_lines(self, lines):
        outfile = SimpleFlatfileImpl(**self.config)
        outfile.write_lines(lines)
        logging.debug("Published %d msgs to all subscribers" % (len(lines))) 
