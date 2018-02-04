import urllib2
import logging
import traceback
import json
from fiery_snap.io.message import Message
from fiery_snap.impl.util.htmlparser_bs4 import ContentHandler
from fiery_snap.processor.base import BaseProcessor
from fiery_snap.impl.util.page import JsonUploadPage, TestPage, ConsumePage
from fiery_snap.impl.util.domain_tlds import possible_domain, filter_domain
import regex

DEBUG_CONTENT = []

DEFANGED = 'defanged'
HASHES = 'hashes'
PROTO = 'proto'
URL = 'url'
LINKS = 'links'
IPS = 'ips'
DOMAINS = 'domains'

NOTE_KEYWORDS = [['phishing', '.*phishing'],
                 ['url', '^url$'],
                 ['url', '^urls$'],
                 ['domain', '^domain$'],
                 ['domain', '^domains$'],
                 ['exploitkit', '^ek'],
                 ['exploitkit', '^exploit kit$'],
                 ['gate', '^gate$'],
                 ['malware', '^malware$']]

ST = '\u2026'

def replace_st(content):
    try:
        x = unicode(content)
        return x.replace(u'\u2026', '')
    except:
        pass
    return content

DF_LINKS = 'defanged_links'
DF_IPS = 'defanged_ips'
DF_DOMAINS = 'defanged_domains'

class TwitterScraper(BaseProcessor):
    ALLOWED_OUT_MESSAGE_KEYS = ['tm_id', 'content', 'defanged_entities', 
                                'entities', 'timestamp', 'keywords',
                                'obtained_timestamp', 'user', 'tags',
                                'linked_content']
    REQUIRED_CONFIG_PARAMS = []
    OPTIONAL_CONFIG_PARAMS = []
    KEY = 'TwitterScraper'
    URL_LOC = 'https://t.co'
    DOMAIN_RE = r'^((?!-))(xn--)?[a-z0-9][a-z0-9-_]{0,61}[a-z0-9]{0,1}\.(xn--)?([a-z0-9\-]{1,61}|[a-z0-9-]{1,30}\.[a-z]{2,})$'
    IP_RE = r'(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))(?![0-9])'
    URI_RE = r"(.\w+:\/\/)?([\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+)"
    URL_RE = r"(h[xXtT][xXtT]p:\/\/|h[xXt][xXt]ps:\/\/)+[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?"
    URL_POT_RE = r"[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?"
    HASH_TAG_RE = r"#(\w+)"
    EMAIL_RE = r"([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)"
    R_DOMAIN_RE = regex.compile(DOMAIN_RE)
    R_IP_RE = regex.compile(IP_RE)
    R_URI_RE = regex.compile(URI_RE)
    R_URL_RE = regex.compile(URL_RE)
    R_URL_POT_RE = regex.compile(URL_POT_RE)
    R_HASH_TAG_RE = regex.compile(HASH_TAG_RE)
    R_EMAIL_RE = regex.compile(EMAIL_RE)
    SIMPLE_MSG = 'From:{source_type}:{source}->' + \
                 '{processor}:{processor_type}\n{safe_hosts}' +\
                 ' {tags}\n(credit {user}: {link})'
    MD5_RE = "([a-fA-F\d]{32})"
    R_MD5_RE = regex.compile(MD5_RE)
    SHA1_RE = "([a-fA-F\d]{40})"
    R_SHA1_RE = regex.compile(SHA1_RE)
    SHA256_RE = "([a-fA-F\d]{64})"
    R_SHA256_RE = regex.compile(SHA256_RE)
    SHA512_RE = "([a-fA-F\d]{128})"
    R_SHA512_RE = regex.compile(SHA512_RE)


    def __init__(self, name, regex_tags=[], regex_keywords=[],
                 regex_keywords_ci=False,
                 simple_message=SIMPLE_MSG, transforms={},
                 extractors={}, processors={}, simple_msg={},
                 subscriber_polling=1.0, message_count=100,
                 publishers={}, subscribers={},
                 service={}, sleep_time=60.0,
                 grab_linked_content=False, 
                 ts_host=None, ts_port=10006):

        super(TwitterScraper, self).__init__(name, transforms=transforms,
                               extractors=extractors, processors=processors,
                               subscriber_polling=subscriber_polling,
                               message_count=message_count,
                               publishers=publishers,
                               subscribers=subscribers,
                               service=service,
                               pages=[JsonUploadPage, TestPage, ConsumePage],
                               sleep_time=sleep_time)

        self.regex_tags = regex_tags
        self.sort_tags()
        self.regex_keywords = regex_keywords
        self.sort_keywords()
        self.regex_keywords_ci = regex_keywords_ci

        self.domain_re = regex.compile(self.DOMAIN_RE)
        self.ip_re = regex.compile(self.IP_RE)
        self.simple_msg = simple_msg.replace('\\\\', '\\')
        self.grab_linked_content = grab_linked_content
        self.topsite_client = None
        if ts_host is not None:
            self.topsite_client = TopSitesCheckClient(host=ts_host, port=ts_port)

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

    @classmethod
    def extract_value_must_contain(cls, _regex, data, mc=['.', '/']):
        res = []
        defanged = []
        # defang and split the string
        #'a.a.a' min size of a string
        content = cls.create_content(data)
        for w, dw in content:
            e = w
            if not all([True if e.find(i) > -1 else False for i in mc]):
                continue

            v = _regex.search(e)
            if v is not None and dw is not None:
                for g in v.captures():
                    defanged.append(g)                
            elif v is not None:
                for g in v.captures():
                    res.append(g)

        return res, defanged

    @classmethod
    def extract_value(cls, _regex, data):
        defanged = []
        res = []
        # defang and split the string
        content = cls.create_content(data)
        #'a.a.a' min size of a string
        # w is orginal term, and dw is the derived term if it was defanged
        for w, dw in content:
            e = w
            if dw is not None:
                e = dw

            v = _regex.search(e)
            if v is not None and dw is not None:
                for g in v.captures():
                    defanged.append(g)
            elif v is not None:
                for g in v.captures():
                    res.append(g)
        return res, defanged

    @classmethod
    def check_and_defang(cls, token):
        defangs = ['[.', '.]', 'hxxp', 'htxp', 'hxtp', '[:', ':]', '[@', '@]']
        token = token.lower()
        for d in defangs:
            if (d != 'hxxp' and token.find(d) > 0) or \
               (d == 'hxxp' and token.find(d) == 0) or \
               (d == 'hxtp' and token.find(d) == 0) or \
               (d == 'htxp' and token.find(d) == 0):
                dt = token.lower()
                dt = dt.replace('[.', '.').replace('.]', '.')
                dt = dt.replace('[:', ':').replace(':]', ':')
                dt = dt.replace('[@', '@').replace('@]', '@')
                dt = dt.replace('hxxp:', 'http:')
                dt = dt.replace('htxp:', 'http:')
                dt = dt.replace('hxtp:', 'http:')
                dt = dt.replace('hxxps:', 'https:')
                dt = dt.replace('htxps:', 'https:')
                dt = dt.replace('hxtps:', 'https:')
                if len(dt) < 4:
                    return False, token
                return True, dt
        return False, token

    @classmethod
    def create_content(cls, line):
        tokens = [w for w in line.split() if len(w) > 5]
        tokens = sorted(set(tokens))

        content = []
        for w in tokens:
            defanged_, dw = cls.check_and_defang(w)
            if not defanged_:
                content.append((w, None))
            else:
                content.append((w, dw))
        return content

    def expand_twitter_urls_extract_content(self, urls):
        expansions = []
        for url in urls:
            # Mongo does not like '.' in names so I need to dodge that for the time being
            # was expansions = {}; expansions[url] = ...
            results = {
                'url': url,
                'failed': True,
                'expanded_link': None,
                'link': None,
                'orig_link': None,
                'content_type': None,
                'content_artifacts': {},
            }
            if not self.grab_linked_content:
                expansions.append(results)
                continue

            try:
                ch = ContentHandler(link=url)
                results['failed'] = False
                # results['content'] = ch.content
                results['expanded_link'] = ch.expanded_link
                results['link'] = ch.link
                results['orig_link'] = url
                results['content_type'] = ch.content_type
                results['content_artifacts'] = ch.extract_potential_artifacts()
                if LINKS in results['content_artifacts']:
                   olinks = results['content_artifacts'][LINKS]
                   nlinks = []
                   for i in olinks:
                       p, l = (i['proto'], i['url'])
                       if l.find('t.co/') == 0:
                           continue
                       u = p
                       p = p.replace('hx', 'ht').replace('xp', 'tp')
                       if p.find('https') > -1:
                          u = 'https'
                       elif p.find('http') > -1:
                          u = 'http'
                       elif p.find('meow') > -1:
                          u = 'http'
                       uri = u + '://' + l
                       domain = urllib2.urlparse.urlsplit(uri).netloc
                       if not filter_domain(domain):
                           nlinks.append(uri)
                       elif domain.replace('.', '').isdigit():
                           nlinks.append(uri)
                   results['content_artifacts'][LINKS] = nlinks
                if DF_LINKS in results['content_artifacts']:
                   olinks = results['content_artifacts'][DF_LINKS]
                   nlinks = []
                   for i in olinks:
                       p, l = (i['proto'], i['url'])
                       if l.find('t.co/') == 0:
                           continue
                       u = p
                       p = p.lower().replace('hx', 'ht').replace('xp', 'tp').lstrip(';').lstrip('>')
                       if p.find('https') > -1:
                          u = 'https'
                       elif p.find('http') > -1:
                          u = 'http'
                       elif p.find('meow') > -1:
                          u = 'http'
                       uri = u + '://' + l
                       domain = urllib2.urlparse.urlsplit(uri).netloc
                       # commenting the following block because defanged 
                       # stuff is not well formed
                       #if not filter_domain(domain):
                       #    nlinks.append(uri)
                       #elif domain.replace('.', '').isdigit():
                       #    nlinks.append(uri)
                       nlinks.append(uri)
                   results['content_artifacts'][DF_LINKS] = nlinks
                if DF_DOMAINS in results['content_artifacts']:
                   od = results['content_artifacts'][DF_DOMAINS]
                   nd = []
                   for domain in od:
                       if not filter_domain(domain):
                           nd.append(domain)
                   results['content_artifacts'][DF_DOMAINS] = nd
                if DOMAINS in results['content_artifacts']:
                   od = results['content_artifacts'][DOMAINS]
                   nd = []
                   for domain in od:
                       if not filter_domain(domain):
                           nd.append(domain)
                   results['content_artifacts'][DOMAINS] = nd
            except:
                logging.debug('Failed with the following exception:\n{}'.format(traceback.format_exc()))
            # Mongo does not like '.' in names so I need to dodge that for the time being
            # was expansions = {}; expansions[url] = ...
            expansions.append(results)
        return expansions

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
        domains, defanged_domains = self.extract_value(self.R_DOMAIN_RE, content)
        ips, defanged_ips = self.extract_value(self.R_IP_RE, content)
        emails, defanged_emails = self.extract_value(self.R_EMAIL_RE, content)
        entities['ips'] = sorted(set(ips))
        defanged_entities['ips'] = sorted(set(defanged_ips))
        entities['emails'] = sorted(set(emails))
        defanged_entities['emails'] = sorted(set(defanged_emails))
        
        entities['domains'] = []
        defanged_entities['domains'] = []
        for domain in domains:
            g = any([ip.find(domain) > -1 for ip in entities['ips']])
            if g:
                continue
            if possible_domain(domain):
                defanged_entities['domains'].append(domain)

        for domain in defanged_domains:
            g = any([ip.find(domain) > -1 for ip in defanged_entities['ips']])
            if g:
                continue
            if possible_domain(domain):
                defanged_entities['domains'].append(domain)

        entities['domains'] = sorted(set(entities['domains']))
        defanged_entities['domains'] = sorted(set(defanged_entities['domains']))
        # entities['domains'] = [i for i in sorted(set(domains)) if len(i) > 6 and not i.replace('.', '').isdigit()]
        # entities['domains'] = [i for i in entities['domains'] if not filter_domain(i)]
        md5, _ = self.extract_value(self.R_MD5_RE, content)
        sha1, _ = self.extract_value(self.R_SHA1_RE, content)
        sha256, _ = self.extract_value(self.R_SHA256_RE, content)
        sha512, _ = self.extract_value(self.R_SHA512_RE, content)
        entities['hashes'] = list(set(md5 + sha1 + sha256 + sha512))


        # entities['uris'] = self.extract_value(self.R_URI_RE, content)
        entities['urls'] = []
        defanged_entities['urls'] = []

        urls, defanged_urls = self.extract_value(self.R_URL_RE, content)
        pot_url_ign = set()
        for u in urls:
            scheme, rest = u.split('://')[0], '://'.join(u.split('://')[1:])
            scheme = scheme.lower().replace('x', 't')
            entities['urls'].append(scheme+'://'+rest)
            pot_url_ign.add(rest)
        
        pot_url_ign = set()
        for u in defanged_urls:
            scheme, rest = u.split('://')[0], '://'.join(u.split('://')[1:])
            scheme = scheme.lower().replace('x', 't')
            defanged_entities['urls'].append(scheme+'://'+rest)
            pot_url_ign.add(rest)

        entities['urls'] = sorted(set(entities['urls']))
        defanged_entities['urls'] = sorted(set(defanged_entities['urls']))

        pot_urls, defanged_pot_urls = self.extract_value_must_contain(self.R_URL_POT_RE, content, mc=['.', '/'])
        pot_urls = [i for i in sorted(set(pot_urls)) if i.find('t.co/') != 0]
        defanged_pot_urls = sorted(set(defanged_pot_urls))

        entities['pot_urls'] = pot_urls # [i for i in pot_urls if not i in pot_url_ign]
        defanged_entities['pot_urls'] = defanged_pot_urls #[i for i in defanged_pot_urls if not i in pot_url_ign]

        hosts = [extract_host(i) for i in entities['urls']] +\
                entities['ips'] + entities['domains']

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
        message['linked_content'] = self.expand_twitter_urls_extract_content(entities['urls'])

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
                if 'domains' in ca and len(ca['domains']) > 0:
                    found_one = True
                    break
                elif 'hashes' in ca and len(ca['hashes']) > 0:
                    found_one = True
                    break
                elif 'ips' in ca and len(ca['ips']) > 0:
                    found_one = True
                    break
                elif 'links' in ca and len(ca['links']) > 0:
                    found_one = True
                    break

            if not found_one:
                linked_content_failed = True

        domains = entities['domains'] + defanged_entities['domains']
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
            elif rdomain in rank_results:
                d = root_results[rdomain]
                domain_results[domain] = d
                continue

            r = self.topsite_client.check(domain)
            results = r.get('results', {})
            rank_results[rdomain] = results
            domain_results[domain] = results
        return domain_results


    def find_hashtags(self, content):
        hti, _ = self.extract_value(self.R_HASH_TAG_RE, content)
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
                   message_count=message_count, subscriber_polling=subscriber_polling,
                   service=service, sleep_time=sleep_time, 
                   grab_linked_content=grab_linked_content)

    def is_empty(self):
        results = {}
        for n, pubsub in self.publishers.items():
            try:
                results[n] = pubsub.is_empty()
            except:
                logging.error("Failed to reset: %s:%s"%(n, type(pubsub)))
                results[n] = None

        for n, pubsub in self.subscribers.items():
            try:
                results[n] = pubsub.is_empty()
            except:
                logging.error("Failed to reset: %s:%s"%(n, type(pubsub)))
                results[n] = None
        return results

    def handle(self, path, data):
        logging.debug ("handling request (%s): %s" % (path, data))
        if path == TestPage.NAME:
            return {'msg': 'success'}
        elif path == JsonUploadPage.NAME:
            return {'msg': "TODO tags and dynamically adding regex for scraping"}
        elif path == ConsumePage.NAME:
            return_posts = 'return_posts' in data
            msg_posts = self.consume_and_publish()
            all_posts = {}
            num_posts = 0
            for k, posts in msg_posts.items():
                all_posts[k] = [i.toJSON() for i in posts]
                num_posts += len(all_posts[k])
            r = {'msg': 'Consumed %d posts'%num_posts, 'all_posts':None}
            if return_posts:
                r['all_posts'] = all_posts
            return r
        return {'error': 'unable to handle message type: %s' % path}
