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
    ALLOWED_OUT_MESSAGE_KEYS = ['paste_key', 'content', 'defanged_entities',
                                'entities', 'timestamp', 'keywords',
                                'obtained_timestamp', 'user', 'tags',
                                'linked_content']
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

    def process_message(self, omessage):
        # local fns for extraction and validatition
        def extract_host(s):
            if len(s.split('://')) > 1:
                s.split('://')[1].split('/')[0]
            return s

        def v_all(v, l):
            return [i for i in l if v(i)]

        message = omessage.copy()
        message.add_field('processor', self.name)
        message.add_field('processor_type', self.class_map_key())
        # get some content from the message and replace \u2026
        content = replace_st(message.get_content())
        DEBUG_CONTENT.append(message)
        # RE extract entities (hosts, domains, IPs, URLs), links
        entities = {}
        defanged_entities = {}
        ds, df_ds = IOCREX.extract_value(consts.DOMAIN, content)
        ips, defanged_ips = IOCREX.extract_value(consts.IP, content)
        emails, defanged_emails = IOCREX.extract_value(consts.EMAIL, content)
        entities[consts.IPS] = sorted(set(ips))
        defanged_entities[consts.IPS] = sorted(set(defanged_ips))
        entities[consts.EMAILS] = sorted(set(emails))
        defanged_entities[consts.EMAILS] = sorted(set(defanged_emails))

        entities[consts.DOMAINS] = []
        defanged_entities[consts.DOMAINS] = []
        for domain in ds:
            g = any([ip.find(domain) > -1 for ip in entities[consts.IPS]])
            if g:
                continue
            if IOCREX.possible_domain(domain):
                defanged_entities[consts.DOMAINS].append(domain)

        for domain in df_ds:
            g = []
            for ip in defanged_entities[consts.IPS]:
                g.append(ip.find(domain) > -1)
            if g:
                continue
            if IOCREX.possible_domain(domain):
                defanged_entities[consts.DOMAINS].append(domain)

        entities[consts.DOMAINS] = sorted(set(entities[consts.DOMAINS]))
        _t = set(defanged_entities[consts.DOMAINS])
        defanged_entities[consts.DOMAINS] = sorted(_t)

        md5, _ = IOCREX.extract_value(consts.MD5, content)
        sha1, _ = IOCREX.extract_value(consts.SHA1, content)
        sha256, _ = IOCREX.extract_value(consts.SHA256, content)
        sha512, _ = IOCREX.extract_value(consts.SHA512, content)
        entities[consts.HASHES] = list(set(md5 + sha1 + sha256 + sha512))

        entities[consts.URLS] = []
        defanged_entities[consts.URLS] = []

        urls, defanged_urls = IOCREX.extract_value(consts.URL, content)
        # pot_url_ign = set()
        # for u in urls:
        #     scheme, rest = u.split('://')[0], '://'.join(u.split('://')[1:])
        #     scheme = scheme.lower().replace('x', 't')
        #     entities[consts.URLS].append(scheme+'://'+rest)
        #     pot_url_ign.add(rest)

        # pot_url_ign = set()
        # for u in defanged_urls:
        #     defanged_entities[consts.URLS].append(scheme+'://'+rest)
        #     pot_url_ign.add(rest)

        entities[consts.URLS] = sorted(set(entities[consts.URLS]))
        df_us = sorted(set(defanged_entities[consts.URLS]))
        defanged_entities[consts.URLS] = df_us

        pus, dpus = IOCREX.extract_value_must_contain(consts.URL_POT,
                                                      content,
                                                      mc=['.', '/'])
        pot_urls = [i for i in sorted(set(pus)) if not is_twitter_co(i)]
        df_pot_urls = [i for i in sorted(set(pus)) if not is_twitter_co(i)]

        entities['pot_urls'] = pot_urls
        defanged_entities['pot_urls'] = df_pot_urls

        hosts = entities[consts.IPS] + entities[consts.DOMAINS]
        for i in entities[consts.URLS]:
            nh = IOCREX.extract_host(i)
            hosts.append(nh)

        safe_hosts = [i.replace('.', '[.]') for i in hosts]
        entities['safe_hosts'] = safe_hosts
        tags = self.find_hashtags(content)
        if 'hashtags' in message['meta']:
            _tags = [i.values()[0] for i in message['meta']['hashtags']]
            tags = sorted(set(tags + _tags))

        keywords = self.find_keywords(content)
        message['tags'] = tags
        message['keywords'] = keywords
        message['entities'] = entities
        message['defanged_entities'] = defanged_entities

        kargs = {}
        kargs.update(message.as_dict())
        kargs.update(entities)
        message['simple_message'] = self.simple_msg.format(**kargs)

        pmsg = Message({})
        for k in self.ALLOWED_OUT_MESSAGE_KEYS:
            pmsg[k] = message.get(k, None)

        # need to limit messages to ones that contain content
        entities = pmsg['entities']
        linked_content = pmsg['linked_content']

        entities_failed = False
        if len(entities["domains"]) == 0 and \
           len(entities["emails"]) == 0 and \
           len(entities["hashes"]) == 0 and \
           len(entities["ips"]) == 0 and \
           len(entities["pot_urls"]) == 0 and \
           len(entities["urls"]) == 0 and \
           len(tags) == 0:
            entities_failed = True

        linked_content_failed = False
        if linked_content is None or len(linked_content) == 0:
            linked_content_failed = True
        else:
            # check to see of domains, ips, hashes, links, exist in any content
            found_one = False
            for info in linked_content:
                ca = info.get('content_artifacts', None)
                if ca is None:
                    continue
                if consts.DOMAINS in ca and len(ca[consts.DOMAINS]) > 0:
                    found_one = True
                    break
                elif consts.HASHES in ca and len(ca[consts.HASHES]) > 0:
                    found_one = True
                    break
                elif consts.IPS in ca and len(ca[consts.IPS]) > 0:
                    found_one = True
                    break
                elif consts.LINKS in ca and len(ca[consts.LINKS]) > 0:
                    found_one = True
                    break

            if not found_one:
                linked_content_failed = True

        domains = entities[consts.DOMAINS] + defanged_entities[consts.DOMAINS]
        pmsg['rankings'] = self.lookup_sites_rank(domains)
        return_none = linked_content_failed and entities_failed
        return None if return_none else pmsg

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

    def find_hashtags(self, content):
        hti, _ = IOCREX.extract_value(consts.HASH_TAG, content)
        hashtags = [i.strip('#') for i in hti]
        return self.hashtags_with_tags(hashtags)

    def find_keywords(self, content):
        found_keywords = set()
        _c = content
        if self.regex_keywords_ci:
            _c = content.lower()

        for rv, regx in self.regex_keywords:
            if rv in found_keywords:
                continue
            v, _ = self.extract_value(regx, _c)
            if len(v) > 0:
                found_keywords.add(rv)
        return list(found_keywords)

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
