from fiery_snap.io.io_base import IOBase

from slackclient import SlackClient


class SimpleSlackClient(object):
    REQUIRED_CONFIG_PARAMS = ['token', 'action', 'channel']
    OPTIONAL_CONFIG_PARAMS = [['as_user', True]]

    def __init__(self, **kargs):
        for k in self.REQUIRED_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k))

        for k, v in self.OPTIONAL_CONFIG_PARAMS:
            setattr(self, k, kargs.get(k, v))

    def post_results(self, results):
        text = results.as_text()
        sc = SlackClient(self.aa)
        try:
            r = sc.api_call(self.action, channel=self.channel,
                            text=text, as_user=self.as_user)
            return r.get('ok', False)
        except Exception as e:
            raise e


class SimpleSlackOutput(IOBase):
    KEY = "simpleslack"
    REQUIRED_CONFIG_PARAMS = ['token', 'action', 'channel']
    OPTIONAL_CONFIG_PARAMS = [['as_user', True], ['name', None]]

    def __init__(self, config_dict):
        IOBase.__init__(self, config_dict)
        self.output_queue = []
        rs = self.random_name(prepend='simple-slack-output')
        self.name = self.config.get('name', rs)

    def write_results(self, message):
        slack_client = self.new_client()
        output = message.get_field('simple_message')
        return slack_client.post_results(output)

    def new_client(self):
        return SimpleSlackClient(**self.config)
