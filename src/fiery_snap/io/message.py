import json


class BaseMessage(object):
    def ack(self):
        pass

    def __init__(self, msg_dict, **kargs):
        self.msg = {}

        for k, v in list(msg_dict.items()):
            self.msg[k] = v

        for k, v in list(kargs.items()):
            self.msg[k] = v

    def copy(self):
        return Message(self.msg)

    def add_field(self, key, value):
        self.msg[key] = value

    def get_content(self):
        return self.msg.get('content', '')

    def as_text(self):
        if hasattr(self.msg, 'repr_fmt'):
            return self.repr_fmt.format(**self.msg)
        return str(self.msg)

    def as_json_text(self):
        return self.toJSON()

    def as_json(self):
        return json.loads(self.toJSON())

    def as_dict(self):
        return self.msg

    def get(self, key, default=None):
        # print key, self.msg.get(key, None)
        return self.msg.get(key, default)

    def __getitem__(self, key):
        # print key, self.msg.get(key, None)
        return self.msg.get(key, None)

    def __setitem__(self, key, value):
        self.msg[key] = value

    def __in__(self, key, value):
        return key in self.msg[key]

    def __str__(self):
        return self.as_json_text()

    def toJSON(self):
        x = json.dumps(self.msg, default=lambda o: o.__dict__ if hasattr(o, '__dict__') else str(o),
            sort_keys=True)
        return x

    @classmethod
    def fromJSON(cls, json_data):
        return cls(**json_data)



class Message(BaseMessage):

    def __init__(self, msg_dict, **kargs):
        BaseMessage.__init__(self, msg_dict, **kargs)
