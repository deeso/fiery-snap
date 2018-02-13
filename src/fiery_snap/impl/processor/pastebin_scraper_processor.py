import logging
import traceback
import json
from fiery_snap.io.message import Message
from fiery_snap.processor.base import BaseProcessor
from fiery_snap.impl.util.page import JsonUploadPage, TestPage, ConsumePage
from ioc_regex.ir import IOCRegex as IOCREX
# from ioc_regex.basic_html_extractor import ContentHandler as CH
from ioc_regex import consts

from fiery_snap.impl.util.topsites_client import TopSitesCheckClient
import regex

DEBUG_CONTENT = []

ST = u'\u2026'


def is_twitter_co(host):
    return host.find('t.co') == 0


def replace_st(content):
    try:
        x = unicode(content)
        return x.replace(ST, '')
    except:
        pass
    return content


class PastebinScraper(BaseProcessor):
    ALLOWED_OUT_MESSAGE_KEYS = ['paste', 'paste_key', 'content', 'defanged_entities',
                                'entities', 'timestamp', 'keywords',
                                'obtained_timestamp', 'user', 'tags', ]
    REQUIRED_CONFIG_PARAMS = []
    OPTIONAL_CONFIG_PARAMS = []
    KEY = 'PastebinScraper'

    SIMPLE_MSG = 'From:{source_type}:{source}->' + \
                 '{processor}:{processor_type}\n{safe_hosts}' +\
                 ' {tags}\n(credit {user}: {link})'

    def __init__(self, name, regex_tags=[], regex_keywords=[],
                 regex_keywords_ci=False,
                 simple_message=SIMPLE_MSG, transforms={},
                 extractors={}, processors={}, simple_msg={},
                 subscriber_polling=1.0, message_count=100,
                 publishers={}, subscribers={},
                 service={}, sleep_time=60.0,
                 grab_linked_content=False,
                 ts_host=None, ts_port=10006):

        sub_polling = subscriber_polling
        super(PastebinScraper, self).__init__(name, transforms=transforms,
                                              extractors=extractors,
                                              processors=processors,
                                              subscriber_polling=sub_polling,
                                              message_count=message_count,
                                              publishers=publishers,
                                              subscribers=subscribers,
                                              service=service,
                                              pages=[JsonUploadPage,
                                                     TestPage,
                                                     ConsumePage],
                                              sleep_time=sleep_time)

        self.regex_tags = regex_tags
        self.sort_tags()
        self.regex_keywords = regex_keywords
        self.sort_keywords()
        self.regex_keywords_ci = regex_keywords_ci

        self.simple_msg = simple_msg.replace('\\\\', '\\')
        self.grab_linked_content = grab_linked_content
        self.topsite_client = None
        if ts_host is not None:
            self.topsite_client = TopSitesCheckClient(host=ts_host,
                                                      port=ts_port)

    def sort_tags(self):
        self.regex_tags = sorted(self.regex_tags, key=lambda x: x[0])
        self.tags = set([i[0] for i in self.regex_tags])

    def sort_keywords(self):
        self.regex_keywords = sorted(self.regex_keywords, key=lambda x: x[0])
        self.keywords = set([i[0] for i in self.regex_keywords])

    def has_keyword(self, keyword):
        return keyword in self.keywords

    def has_tag(self, tag):
        return tag in self.tags

    def get_keywords(self, keyword):
        if self.has_keyword(keyword):
            return [[n, regx.pattern] for n, regx in self.regex_keywords if n == keyword]
        return []

    def get_tags(self, tag):
        if self.has_tag(tag):
            return [[n, regx.pattern] for n, regx in self.regex_tags if n == tag]
        return []

    def remove_tag(self, tag):
        # if tag in self.tags:
        # t = [i for i in self.regex_tags if i[0] != tag]
        # self.regex_tags = t
        pass

    def add_tag_regex(self, tag, tags_regex):
        pair = (tag, regex.compile(tags_regex))
        self.regex_tags.append(pair)
        self.sort_tags()

    def add_tags(self, tags_regex_list):
        for tag, trex in tags_regex_list:
            pair = (tag, regex.compile(trex))
            self.regex_tags.append(pair)

    def extract_contents(self, content):
        # get some content from the message and replace \u2026
        # DEBUG_CONTENT.append(content)
        # RE extract entities (hosts, domains, IPs, s), links
        rexk = self.regex_keywords
        entities = IOCREX.extract_all_possible(content,
                                               addl_keywords=rexk)

        domains = entities[consts.DOMAIN] + entities[consts.DF_DOMAIN]
        hashes_s = [consts.MD5, consts.SHA1,
                    consts.SHA256, consts.SHA512]

        for k in hashes_s:
            del entities[k]

        tags = [i.strip('#') for i in entities[consts.HASH_TAG]]
        entities[consts.HASH_TAG] = tags
        entities['processed_tags'] = self.hashtags_with_tags(tags)
        entities['site_rankings'] = self.lookup_sites_rank(domains)

        # us = self.expand_twitter_urls_extract_content(entities[consts.URL])
        # entities['linked_content'] = us

        keys = [consts.DOMAIN, consts.DF_DOMAIN]
        for k in keys:
            new_items = []
            _t = entities.get(k, [])
            new_items = [d for d in _t if not IOCREX.filter_domain(d)]
            entities[k] = new_items

        keys = [consts.URL, consts.DF_URL, consts.URL_POT, consts.DF_URL_POT]
        for k in keys:
            new_items = []
            urls = entities.get(k, [])
            _s = zip(urls,
                     IOCREX.hosts_from_urls(urls, True))
            new_items = [u for u, d in _s if not IOCREX.filter_domain(d)]
            entities[k] = new_items

        edip = entities[consts.DF_DOMAIN] + entities[consts.DF_IP]
        adip = []  # us[consts.DF_DOMAIN] + us[consts.DF_IP]
        ht = entities[consts.HASH_TAG]  # + us[consts.HASH_TAG]
        kw = entities[consts.KEYWORDS]  # + us[consts.KEYWORDS]
        safe_hosts = [i.replace('.', '[.]') for i in edip + adip]

        entities[consts.HASH_TAG] = ht
        entities[consts.KEYWORDS] = kw
        entities['safe_hosts'] = safe_hosts
        good_message = IOCREX.is_good_result(entities)
        return good_message, entities

    def process_message(self, omessage):
        # local fns for extraction and validatition
        kargs = {}
        content = replace_st(omessage.get_content())
        paste = omessage['meta']['paste']
        paste_key = omessage['meta']['paste_key']

        good_message, entities = self.extract_contents(content)
        message = omessage.copy()
        message.add_field('processor', self.name)
        message.add_field('processor_type', self.class_map_key())
        message['version'] = '323'
        message['entities'] = entities
        # message['linked_content'] = entities['linked_content']
        # del entities['linked_content']

        message['tags'] = entities['processed_tags']
        del entities['processed_tags']

        kargs['safe_hosts'] = entities['safe_hosts']
        del entities['safe_hosts']

        kargs.update(message.as_dict())
        message['simple_message'] = self.simple_msg.format(**kargs)
        message['paste'] = paste
        message['paste_key'] = paste_key
        message['tm_id'] = paste_key
        pmsg = Message({})
        for k in self.ALLOWED_OUT_MESSAGE_KEYS:
            pmsg[k] = message.get(k, None)
        # pmsg['entities'] = entities

        # need to limit messages to ones that contain content
        return None if not good_message else pmsg

    def root_domain(self, domain):
        c = domain.strip('.')
        if len(c.split('.')) > 1:
            return ".".join(c.split('.')[-2:])
        return None

    def lookup_sites_rank(self, domains):
        domain_results = {}
        root_results = {}
        if self.topsite_client is None:
            return domain_results
        for domain in domains:
            rdomain = self.root_domain(domain)
            if rdomain is None:
                continue
            elif domain in domain_results:
                d = root_results[rdomain]
                domain_results[domain] = d
                continue

            r = self.topsite_client.check(domain)
            results = r.get('results', {})
            domain_results[rdomain] = results
            domain_results[domain] = results
        return domain_results

    def hashtags_with_tags(self, hashtags):
        nhashtags = set()
        for tag in hashtags:
            for name, tre in self.regex_tags:
                if tre.search(tag) is not None:
                    nhashtags.add(name)
                    break
        return list(nhashtags)

    def process(self, msgs):
        results = []
        omsgs = []
        for m in msgs:
            try:
                omsgs.append(json.loads(m))
            except:
                omsgs.append(m)
        for m in omsgs:
            nmessage = self.process_message(m)
            if nmessage is not None:
                self.add_outgoing_message(nmessage)
                results.append(nmessage)
        return results

    def process_task(self, body, kombu_message):
        jsonmsg = json.loads(kombu_message.payload)
        r = self.process_message(jsonmsg)
        if r is not None:
            self.publish(r)
        kombu_message.ack()

    @classmethod
    def parse(cls, config_dict, **kargs):
        name = config_dict.get('name', None)
        nextractors = config_dict.get('extractors', [])
        ntransforms = config_dict.get('transforms', [])
        subscribers = config_dict.get('subscribers', {})
        publishers = config_dict.get('publishers', {})
        message_count = config_dict.get('message_count', 100)
        topsite_client_block = config_dict.get('top-sites-check', None)
        subscriber_polling = config_dict.get('subscriber_polling', 1.0)

        regex_tags = kargs.get('regex_tags', [])
        regex_keywords = kargs.get('regex_keywords', [])
        regex_keywords_ci = kargs.get('regex_keywords_ci', True)

        simple_msg = config_dict.get('simple_msg', {})
        sleep_time = config_dict.get('sleep_time', 60.0)
        xforms = kargs.get('xforms', {})
        xtracts = kargs.get('xtracts', {})
        extractors = dict([(i, xforms.get(i)) for i in nextractors
                          if i in xtracts])
        transforms = dict([(i, xforms.get(i)) for i in ntransforms
                          if i in xforms])
        service = config_dict.get('service', {})
        grab_linked_content = config_dict.get('grab_linked_content', False)

        return cls(name, transforms=transforms, extractors=extractors,
                   regex_tags=regex_tags, regex_keywords=regex_keywords,
                   regex_keywords_ci=regex_keywords_ci,
                   simple_msg=simple_msg,
                   subscribers=subscribers, publishers=publishers,
                   message_count=message_count,
                   subscriber_polling=subscriber_polling,
                   service=service, sleep_time=sleep_time,
                   grab_linked_content=grab_linked_content)

    def is_empty(self):
        results = {}
        for n, pubsub in self.publishers.items():
            try:
                results[n] = pubsub.is_empty()
            except:
                logging.error("Failed to reset: %s:%s" % (n, type(pubsub)))
                results[n] = None

        for n, pubsub in self.subscribers.items():
            try:
                results[n] = pubsub.is_empty()
            except:
                logging.error("Failed to reset: %s:%s" % (n, type(pubsub)))
                results[n] = None
        return results

    def handle(self, path, data):
        logging.debug("handling request (%s): %s" % (path, data))
        if path == TestPage.NAME:
            return {'msg': 'success'}
        elif path == JsonUploadPage.NAME:
            _m = "TODO tags and dynamically adding regex for scraping"
            return {'msg': _m}
        elif path == ConsumePage.NAME:
            return_posts = 'return_posts' in data
            msg_posts = self.consume_and_publish()
            all_posts = {}
            num_posts = 0
            for k, posts in msg_posts.items():
                all_posts[k] = [i.toJSON() for i in posts]
                num_posts += len(all_posts[k])
            r = {'msg': 'Consumed %d posts' % num_posts, 'all_posts': None}
            if return_posts:
                r['all_posts'] = all_posts
            return r
        return {'error': 'unable to handle message type: %s' % path}
