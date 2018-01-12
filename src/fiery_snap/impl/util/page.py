import logging
import json
import web

class Page:
    BACKEND = {}
    NAME = ''
    CNAME = 'Page'
    @classmethod
    def mapping(cls):
        return ['/'+cls.NAME, cls.CNAME]

    def GET(self):
        data = web.input()
        # path = web.ctx.path
        base = web.ctx.path.split('/')[0]
        # print data
        # data = json.loads(data)
        web.header('Content-Type', 'application/json')
        if base in self.BACKEND:
            r = self.BACKEND[base].handle(self.NAME, data)
            return json.dumps(r)

        return json.dumps({'error':'can not handle this request type'})

    def POST(self):
        data = web.data()
        json_data = json.loads(data)
        web.header('Content-Type', 'application/json')
        logging.debug("Recieved a request for: %s" % self.NAME)
        # base = web.ctx.path.split('/')[0]
        base = '' if 'target' not in json_data else json_data['target']
        # print data
        data = json.loads(data)
        web.header('Content-Type', 'application/json')
        if base in self.BACKEND:
            logging.debug("Handled (%s) request for: %s" % (self.NAME, base))
            r = self.BACKEND[base].handle(self.NAME, json_data)
            
            return json.dumps(r)
        target = base if len(base) > 0 else 'TARGET not specified'
        logging.debug("Failed to handle request for: %s" % self.NAME)
        return json.dumps({'error':'%s can not handle this request type for "%s"' % (self.NAME, target)})
    @classmethod
    def set_back_end(cls, back_end):
        cls.BACKEND = back_end

class AddHandlesPage(Page):
    NAME = 'add_handles'
    CNAME = "AddHandlesPage"
    def GET(self):
        data = web.input()
        # print data
        # data = json.loads(data)
        json_data = {}
        if 'handle' in data:
            json_data = {'handles': data.get('handle').split(',')}
        if 'handles' in data:
            json_data = {'handles': data.get('handles').split(',')}

        if self.BACKEND is not None:
            web.header('Content-Type', 'application/json')
            return self.BACKEND.handle(self.NAME, json_data)

        return {'error':'back_end does not handle this request type'}

class RemoveHandlesPage(Page):
    NAME = 'rm_handles'
    CNAME = "RemoveHandlesPage"
    def GET(self):
        data = web.input()
        # print data
        # data = json.loads(data)
        json_data = {}
        if 'handle' in data:
            json_data = {'handles': data.get('handle').split(',')}
        if 'handles' in data:
            json_data = {'handles': data.get('handles').split(',')}

        if self.BACKEND is not None:
            return self.BACKEND.handle(self.NAME, json_data)

        return {'error':'back_end does not handle this request type'}

class ListHandlesPage(Page):
    NAME = 'list_handles'
    CNAME = "ListHandlesPage"

class ShutdownPage(Page):
    NAME = 'shutdown'
    CNAME = "ShutdownPage"

class ConsumePage(Page):
    NAME = 'consume'
    CNAME = "ConsumePage"

class TestPage(Page):
    NAME = 'testpage'
    CNAME = "TestPage"

class JsonUploadPage(Page):
    NAME = 'jsonupload'
    CNAME = "JsonUploadPage"

class MongoSearchPage(Page):
    NAME = 'mongosearch'
    CNAME = "MongoSearchPage"

class EmailInfoPage(Page):
    NAME = 'emailinfopage'
    CNAME = "EmailInfoPage"

