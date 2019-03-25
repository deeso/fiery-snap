import logging
from fiery_snap.impl.util.page import *
import threading
import requests
import web

import time
import ctypes

#  https://gist.github.com/liuw/2407154
def ctype_async_raise(thread_obj, exception):
    found = False
    target_tid = 0
    for tid, tobj in list(threading._active.items()):
        if tobj is thread_obj:
            found = True
            target_tid = tid
            break

    if not found:
        raise ValueError("Invalid thread object")

    ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(target_tid), 
                                                     ctypes.py_object(exception))
    # ref: http://docs.python.org/c-api/init.html#PyThreadState_SetAsyncExc
    if ret == 0:
        raise ValueError("Invalid thread ID")
    elif ret > 1:
        # Huh? Why would we notify more than one threads?
        # Because we punch a hole into C level interpreter.
        # So it is better to clean up the mess.
        ctypes.pythonapi.PyThreadState_SetAsyncExc(target_tid, NULL)
        raise SystemError("PyThreadState_SetAsyncExc failed")
    logging.error("Successfully set asynchronized exception for: %d"%target_tid)

class GenericService(object):
    def __init__(self, name='GenericService', back_end=None, 
                 listening_address="0.0.0.0",
                 listening_port=None, 
                 pages=[],
                 page_objs=[], 
                 **kargs):
        
        if back_end is None:
            raise Exception("Backend of the service must be specified")
        elif listening_port is None:
            raise Exception("Listening port of the service must be specified")
        elif len(pages) == 0 and len(page_objs) == 0:
            raise Exception("Specify at least one page to handle")

        self.name = name if kargs.get('service_name', None ) is None else kargs.get('service_name')
        self.back_end = back_end
        self.listening_address = listening_address
        self.listening_port = listening_port
        self.pages = pages
        for o in page_objs:
            o.BACKEND[self.name] = self.back_end
            self.pages = self.pages + o.mapping()

        self.app = None
        self.t = None

    def get_base_url(self, target=''):
        return 'http://{}:{}/{}'.format(self.listening_address, 
                                           self.listening_port,
                                           target)

    def is_alive(self):
        if self.t is None:
            return False
        return self.t.isAlive()

    def create_app(self):
        class GSApp(web.application):
            def run(self, *middleware):
                service = getattr(self, 'service', None)
                port = service.listening_port if service is not None else 20202
                addr = service.listening_address if service is not None else ""
                func = self.wsgifunc(*middleware)
                return web.httpserver.runsimple(func, (addr, port))

        
        if self.listening_address is None or \
           self.listening_port < 1 or \
           self.back_end is None:
           return
        # avoid static class property
        self.app = GSApp(self.pages, globals())
        setattr(self.app, "service", self)
        


    def dummy_req(self):
        try:
            uri = "http://{}:{}".format(self.listening_address, self.listening_port)
            r = requests.get(uri)
        except:
            pass

    def start(self):
        if self.t is not None and self.t.isAlive():
            return False
        self.create_app()
        if self.app is None:
            logging.debug("Unable to set the App for web.py for %s service"%self.name)
            raise Exception("Unable to set the App for web.py")
        self.t = threading.Thread(target=self.app.run)
        self.t.start()
        setattr(self.app, "SERVER", None)
        # Race Condition here, so we wait patiently
        while self.app.SERVER is None:
            try:
                from web.httpserver import server as WEBPY_SERVER
                setattr(self.app, "SERVER", WEBPY_SERVER)
            except:
                logging.debug("Failed to set app server from ")
                time.sleep(1.0)

        logging.debug("Started %s service"%self.name)
        return True

    def stop(self):
        if self.app and self.t.isAlive():
            logging.debug("Stopping %s service"%self.name)
            self.app.stop()
            if self.app.SERVER is not None:
                self.app.stop()
                self.app.SERVER.stop()

            time.sleep(2.0)
            if self.t.isAlive():
                self.app.stop()
                try:
                    # meh trigger an exception in the threads for certain death
                    logging.debug("... %s service still living"%self.name)
                    logging.debug("... forcing exception")
                    if self.app.SERVER is not None and self.app.SERVER.is_alive():
                        ctype_async_raise(self.app.SERVER, Exception("die"))                    
                except:
                    pass
                try:
                    if self.t is not None and self.t.isAlive():
                        ctype_async_raise(self.t, Exception("die"))
                except:
                    pass


            self.app.stop()
            time.sleep(2.0)
            self.t.join()
            self.app = None

    def handle(self, pagename, json_data):
        result = {'error': 'unable to handle message'}
        if pagename == TestPage.NAME:
            result = {'pagename': pagename,
                'pong': json_data.get('pong', 'failed')}
        return result