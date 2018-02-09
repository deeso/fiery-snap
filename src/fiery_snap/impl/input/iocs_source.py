import base64
from fiery_snap.io.io_base import IOBase
from fiery_snap.io.connections import *
from ioc_regex.basic_html_extractor import ContentHandler as CH
from fiery_snap.impl.input.kombu_source import KombuClientProducer
from fiery_snap.impl.util.page import JsonUploadPage, TestPage


class DomainSource(KombuClientProducer):
    KEY = 'domain-source'

    def handle(self, path, data):
        if path == JsonUploadPage.NAME:
            domains = data.get('domains', [])
            domain = data.get('domain', None)
            if domain is not None or len(domain) > 0:
                domains.append(domain)

            source = data.get('source', 'json_upload')
            source_type = data.get('source_type', 'upload_service')
            messages = []
            t = 'domain'
            for v in domains:
                msg = {
                        'source': source,
                        'source_type': source_type,
                        'value': v,
                        'type': t
                     }
                messages.append(msg)

            if len(messages) > 0:
                self.publish_all_msgs(messages)
            return {'msg': 'published %d messages in the queue'}
        return {'error': 'unable to handle message type: %s' % path}


class HostSource(KombuClientProducer):
    KEY = 'host-source'
    REQUIRED_CONFIG_PARAMS = ['name',
                              'uri',
                              'queue_name',
                              'publishers',
                              'subscribers',
                              ]
    OPTIONAL_CONFIG_PARAMS = []

    def __init__(self, config_dict, **kargs):
        IOBase.__init__(self, config_dict, pages=[JsonUploadPage, TestPage])
        self.my_queue = KombuPubSubConnection(config_dict.get('name'),
                                              config_dict.get('uri'),
                                              config_dict.get('queue_name'),
                                              iobase=self)

    def handle(self, path, data):
        if path == JsonUploadPage.NAME:
            domains = data.get('domains', [])
            domain = data.get('domain', None)
            if domain is not None or len(domain) > 0:
                domains.append(domain)

            ips = data.get('ips', [])
            ip = data.get('ip', None)
            if ip is not None or len(ip) > 0:
                ips.append(ip)

            source = data.get('source', 'json_upload')
            source_type = data.get('source_type', 'upload_service')
            t = 'domain'
            for v in domains:
                msg = {
                        'source': source,
                        'source_type': source_type,
                        'value': v,
                        'type': t
                     }
                messages.append(msg)

            t = 'ip'
            for v in ips:
                msg = {
                        'source': source,
                        'source_type': source_type,
                        'value': v,
                        'type': t
                     }
                messages.append(msg)

            if len(messages) > 0:
                self.publish_all_msgs(messages)
            return {'msg': 'published %d messages in the queue'}
        return {'error': 'unable to handle message type: %s' % path}


class IpSource(KombuClientProducer):
    KEY = 'ip-source'
    REQUIRED_CONFIG_PARAMS = ['name',
                              'uri',
                              'queue_name',
                              'publishers',
                              'subscribers']
    OPTIONAL_CONFIG_PARAMS = []

    def __init__(self, config_dict, **kargs):
        IOBase.__init__(self, config_dict, pages=[JsonUploadPage, TestPage])
        self.my_queue = KombuPubSubConnection(config_dict.get('name'),
                                              config_dict.get('uri'),
                                              config_dict.get('queue_name'),
                                              iobase=self)

    def handle(self, path, data):
        if path == JsonUploadPage.NAME:
            domains = data.get('domains', [])
            domain = data.get('domain', None)
            if domain is not None or len(domain) > 0:
                domains.append(domain)

            ips = data.get('ips', [])
            ip = data.get('ip', None)
            if ip is not None or len(ip) > 0:
                ips.append(ip)

            source = data.get('source', 'json_upload')
            source_type = data.get('source_type', 'upload_service')
            t = 'ip'
            for v in ips:
                msg = {
                        'source': source,
                        'source_type': source_type,
                        'value': v,
                        'type': t
                     }
                messages.append(msg)

            if len(messages) > 0:
                self.publish_all_msgs(messages)
            return {'msg': 'published %d messages in the queue'}
        return {'error': 'unable to handle message type: %s' % path}


class HashSource(KombuClientProducer):
    KEY = 'hash-source'
    REQUIRED_CONFIG_PARAMS = ['name',
                              'uri',
                              'queue_name',
                              'publishers',
                              'subscribers']
    OPTIONAL_CONFIG_PARAMS = []

    def __init__(self, config_dict, **kargs):
        IOBase.__init__(self, config_dict, pages=[JsonUploadPage, TestPage])
        self.my_queue = KombuPubSubConnection(config_dict.get('name'),
                                              config_dict.get('uri'),
                                              config_dict.get('queue_name'),
                                              iobase=self)

    def handle(self, path, data):
        if path == JsonUploadPage.NAME:
            hashes = data.get('hashes', [])
            hash_ = data.get('hash', None)
            if hash_ is not None or len(hash_) > 0:
                hashes.append(hash_)

            source = data.get('source', 'json_upload')
            source_type = data.get('source_type', 'upload_service')
            t = 'hashes'
            for v in hashes:
                msg = {
                        'source': source,
                        'source_type': source_type,
                        'value': v,
                        'type': t
                     }
                messages.append(msg)

            if len(messages) > 0:
                self.publish_all_msgs(messages)
            return {'msg': 'published %d messages in the queue'}
        return {'error': 'unable to handle message type: %s' % path}


class HtmlSource(KombuClientProducer):
    KEY = 'html-source'
    REQUIRED_CONFIG_PARAMS = ['name',
                              'uri',
                              'queue_name',
                              'publishers',
                              'subscribers', ]
    OPTIONAL_CONFIG_PARAMS = []

    def __init__(self, config_dict, **kargs):
        IOBase.__init__(self, config_dict, pages=[JsonUploadPage, TestPage])
        self.my_queue = KombuPubSubConnection(config_dict.get('name'),
                                              config_dict.get('uri'),
                                              config_dict.get('queue_name'),
                                              iobase=self)

    def handle(self, path, data):
        if path == JsonUploadPage.NAME:
            urls = data.get('urls', [])
            url = data.get('url', None)
            if url is not None or len(url) > 0:
                urls.append(url)

            source = data.get('source', 'json_upload')
            source_type = data.get('source_type', 'upload_service')
            t = 'urls'
            for v in hashes:
                msg = {
                        'source': source,
                        'source_type': source_type,
                        'value': v,
                        'type': t,
                        'html': None,
                        'error': None,
                        'expanded_link': False,
                        'content_url': None,
                     }
                try:
                    ch = ContentHandler(link=v)
                    k = base64.encodestring(ch.content).replace('\n', '')
                    msg['html_b64'] = k
                    msg['expanded_link'] = ch.expanded_link
                    msg['content_url'] = ch.link
                except:
                    msg['error'] = "Failed to retrieve content"

                messages.append(msg)

            if len(messages) > 0:
                self.publish_all_msgs(messages)
            return {'msg': 'published %d messages in the queue'}
        return {'error': 'unable to handle message type: %s' % path}
