import requests


class TopSitesCheckClient(object):

    FORMAT = "http://{host}:{port}"
    UPDATE = "/topsites/update"
    CHECK = "/topsites/check/{domain}"
    INDEX = "/"

    def __init__(self, host='127.0.0.1', port=10006):
        self.host = host
        self.port = port

    def check(self, domain):
        kargs = {
                 'host': self.host,
                 'port': self.port,
                 'domain': domain
                }
        url = (self.FORMAT + self.CHECK).format(**kargs)
        print (url)
        rsp = requests.get(url)
        return rsp.json()

    def update(self):
        kargs = {
                 'host': self.host,
                 'port': self.port,
                }
        url = (self.FORMAT + self.UPDATE).format(**kargs)
        print (url)
        rsp = requests.get(url)
        return rsp.json()

    def ping(self):
        kargs = {
                 'host': self.host,
                 'port': self.port,
                }
        url = (self.FORMAT + self.INDEX).format(**kargs)
        rsp = requests.get(url)
        return rsp.text == 'works'
