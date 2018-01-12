import urllib2
import logging
from bs4 import BeautifulSoup, SoupStrainer
import requests
import regex
import json
from .domain_tlds import possible_domain

DEFANGED = 'defanged'
HASHES = 'hashes'
PROTO = 'proto'
URL = 'url'
LINKS = 'links'
IPS = 'ips'
DOMAINS = 'domains'

DF_LINKS = 'defanged_links'
DF_IPS = 'defanged_ips'
DF_DOMAINS = 'defanged_domains'

class ContentHandler(object):
    URI_RE = r"(.\w+:\/\/)?([\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+)"
    R_URI_RE = regex.compile(URI_RE)
    IP_RE = r'(?<![0-9])(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))(?![0-9])'
    R_IP_RE = regex.compile(IP_RE)
    DOMAIN_RE = r'((?!-))(xn--)?[a-z0-9][a-z0-9-_]{0,61}[a-z0-9]{0,1}\.(xn--)?([a-z0-9\-]{1,61}|[a-z0-9-]{1,30}\.[a-z]{2,})$'
    R_DOMAIN_RE = regex.compile(DOMAIN_RE)
    MD5_RE = "([a-fA-F\d]{32})"
    R_MD5_RE = regex.compile(MD5_RE)
    SHA1_RE = "([a-fA-F\d]{40})"
    R_SHA1_RE = regex.compile(SHA1_RE)
    SHA256_RE = "([a-fA-F\d]{64})"
    R_SHA256_RE = regex.compile(SHA256_RE)
    SHA512_RE = "([a-fA-F\d]{128})"
    R_SHA512_RE = regex.compile(SHA512_RE)

    def __init__(self, link=None, content=None, look_at_embedded_links=True):
        if content is None and link is None:
            raise Exception("Provide either a link or content to analyze")

        self.link = link
        self.expanded_link = False
        self.orig_link = link
        self.content = content
        self.content_type = 'html'
        self.response = None
        self.bs4_parser = None
        if self.link is not None and self.content is None:
            # download the link
            self.response = requests.get(self.link)
            self.content_type = self.response.headers['content-type']
            # read the contents
            if self.response.status_code == 200:
                self.content = self.response.text
                self.link = self.response.request.url
                logging.debug("Expanded link to: %s"%self.link)
                self.expanded_link = self.orig_link != self.link
            else:
                raise Exception("Unable to get the specified content: HTTP STATUS CODE = %d"%self.response.status_code)

        if self.content_type.find('html') > -1 or \
            self.content_type.find('text/plain') > -1:
            self.bs4_parser = BeautifulSoup(self.content, 'html.parser')
        elif self.content_type.find('json'):
            # create key value mappings line by line
            lines = []
            json_data = json.loads(self.content)
            self.content = json.dumps(json_data, indent=0, sort_keys=True)
            self.bs4_parser = BeautifulSoup(self.content, 'html.parser')

        self.embedded_links = self.extract_embedded_links()

        self.artifacts = self.extract_potential_artifacts()

    @classmethod
    def create_content(cls, line):
        tokens = [w for w in line.split() if len(w) > 5]
        content = []
        for w in tokens:
            defanged_, dw = cls.check_and_defang(w)
            if not defanged_:
                content.append((w, None))
            else:
                content.append((w, dw))
        return content

    @classmethod
    def extract_domain_or_host(cls, line, defanged_only=False):
        r = {IPS:[], DOMAINS:[], DF_DOMAINS:[], DF_IPS:[]}
        content = cls.create_content(line)
        for w, dw in content:
            domain = cls.R_DOMAIN_RE.search(w)
            ip = cls.R_IP_RE.search(w)
            if ip is not None and dw is None:
                d = ip.captures()[0]
                r[IPS].append(d)
            elif ip is not None:
                d = ip.captures()[0]
                r[DF_IPS].append(d)

            if domain is not None and dw is not None:
                d = domain.captures()[0]
                # check domain tlds first
                if possible_domain(d):
                    r[DF_DOMAINS].append(d)
            elif domain is not None:
                d = domain.captures()[0]
                # check domain tlds first
                if possible_domain(d):
                    r[DOMAINS].append(d)

        return r

    @classmethod
    def extract_link(cls, line):
        r = {LINKS:[], DF_LINKS: []}

        content = cls.create_content(line)
        for w, dw in content:
            f = cls.R_URI_RE.search(w)
            if w.find('http://') == 0 or w.find('https://') == 0:
                proto, url = w.split('://')
                r[DF_LINKS].append({PROTO: proto.strip('"').strip("'"), URL:url})
            elif f is not None and dw is not None:
                proto, url = f.groups()
                if proto is not None:
                    proto = proto.strip('://')
                    r[DF_LINKS].append({PROTO: proto.strip('"').strip("'"), URL:url})
            elif f is not None:
                proto, url = f.groups()
                if proto is not None:
                    proto = proto.strip('://')
                    r[LINKS].append({PROTO: proto.strip('"').strip("'"), URL:url})
        return r

    @classmethod
    def extract_hash(cls, line):
        r = {
                HASHES:[],
                'md5':[],
                'sha512':[],
                'sha256':[],
                'sha1':[],
            }
        for w in line.split():
            md5 = cls.R_MD5_RE.search(w)
            sha1 = cls.R_SHA1_RE.search(w)
            sha256 = cls.R_SHA256_RE.search(w)
            sha512 = cls.R_SHA512_RE.search(w)
            h = None
            if sha512 is not None:
                h = sha512.captures()[0]
                r['sha512'].append(h)
            if sha256 is not None:
                h = sha256.captures()[0]
                r['sha256'].append(h)
            if sha1 is not None:
                h = sha1.captures()[0]
                r['sha1'].append(h)
            if md5 is not None:
                h = md5.captures()[0]
                r['md5'].append(h)
            if h is not None:
                r[HASHES].append(h)
        return r

    def extract_embedded_links(self):
        urls = set()
        results = {LINKS:[], DOMAINS:[], IPS:[]}
        for link in BeautifulSoup(self.content, 'html.parser',
                                  parse_only=SoupStrainer('a')):
            line = None
            if 'href' in link:
                line = link['attrs']['href']

            if line is None or len(line) < 3:
                continue

            extracted_links = self.extract_link(line)            
            results[LINKS] = results[LINKS] + extracted_links[LINKS]
            results[DF_LINKS] = results[DF_LINKS] + extracted_links[DF_LINKS]

            if len(extracted_links[DF_LINKS]) > 0:
                for info in extracted_links[DF_LINKS]:
                    url = info[URL]
                    if url in urls:
                        continue
                    urls.add(url)

                    hi = self.extract_domain_or_host(url)
                    results[DF_IPS] = results[DF_IPS] + [i for i in hi[DF_IPS]] 
                    results[DF_DOMAINS] = results[DF_DOMAINS] + [i for i in hi[DF_DOMAINS]]
                    results[DF_IPS] = results[IPS] + [i for i in hi[IPS]] 
                    results[DF_DOMAINS] = results[DOMAINS] + [i for i in hi[DOMAINS]]

            if len(extracted_links[LINKS]) > 0:
                for info in extracted_links[LINKS]:
                    url = info[URL]
                    if url in urls:
                        continue
                    urls.add(url)

                    hi = self.extract_domain_or_host(url)
                    results[IPS] = results[IPS] + [i for i in hi[IPS]] 
                    results[DOMAINS] = results[DOMAINS] + [i for i in hi[DOMAINS]]
        return results

    @classmethod
    def check_and_defang(self, token):
        defangs = ['[.', '.]', 'hxxp', '[:', ':]', '[@', '@]']
        for d in defangs:
            if (d != 'hxxp' and token.find(d) > 0) or \
               (d == 'hxxp' and token.find(d) == 0):
                dt = token.lower()
                dt = dt.replace('[.', '.').replace('.]', '.')
                dt = dt.replace('[:', ':').replace(':]', ':')
                dt = dt.replace('[@', '@').replace('@]', '@')
                dt = dt.replace('hxxp:', 'http:')
                if len(dt) < 4:
                    return False, token
                return True, dt
        return False, token

    def extract_potential_artifacts(self):
        return self.extract_all()

    def get_host_info_update(self, line, results, ip_seen=set(), domain_seen=set()):
        hi = self.extract_domain_or_host(line.lower())

        results[DF_IPS] = sorted(set(results[DF_IPS] + hi[DF_IPS]))
        results[DF_DOMAINS] = sorted(set(results[DF_DOMAINS] + hi[DF_DOMAINS]))
        results[IPS] = sorted(set(results[IPS] + hi[IPS]))
        results[DOMAINS] = sorted(set(results[DOMAINS] + hi[DOMAINS]))

        return results

    def extract_all_defanged(self):
        lines = [i.strip() for i in self.content.splitlines() if len(i.strip()) > 0]
        link_seen = set()
        ip_seen = set()
        hashes_seen = set()
        domain_seen = set()
        results = {
                    HASHES:[],
                    DOMAINS:[],
                    LINKS:[],
                    IPS: [],
                    DF_IPS: [],
                    DF_DOMAINS: [],
                    DF_LINKS: [],
                }

        for line in lines:
            results = self.get_host_info_update(line, results, ip_seen, domain_seen)
            uinfos = self.extract_link(line)
            if len(uinfos[LINKS]) > 0:
                g = [i for i in uinfos[LINKS] if i[URL] not in link_seen]
                results[LINKS] = results[LINKS] + g
                link_seen |= set([i[URL] for i in g])
            
            if len(uinfos[DF_LINKS]) > 0:
                g = [i for i in uinfos[DF_LINKS] if i[URL] not in link_seen]
                results[DF_LINKS] = results[DF_LINKS] + g
                link_seen |= set([i[URL] for i in g])

            hinfo = self.extract_hash(line)
            if len(hinfo[HASHES]) > 0:
                g = [i for i in hinfo[HASHES] if i not in hashes_seen]
                results[HASHES] = results[HASHES] + g
                hashes_seen |= set(g)

        # update urls and such from the urls
        for link in results[LINKS]:
            uri = link[PROTO] + '://' + link[URL]
            domain = urllib2.urlparse.urlsplit(uri).netloc
            # print "uri: %s domain: %s"% (uri, domain)
            if possible_domain(domain):
                results[DOMAINS].append(domain)
            results = self.get_host_info_update(link[URL], results, ip_seen, domain_seen)
        
        for link in results[DF_LINKS]:
            uri = link[PROTO] + '://' + link[URL]
            domain = urllib2.urlparse.urlsplit(uri).netloc
            # print "uri: %s domain: %s"% (uri, domain)
            if possible_domain(domain):
                results[DF_DOMAINS].append(domain)
            results = self.get_host_info_update(link[URL], results, ip_seen, domain_seen)
        
        clean_results = {
                    HASHES:sorted(set(results[HASHES])),
                    LINKS:[],
                    DOMAINS:sorted(set(results[DOMAINS])),
                    IPS: sorted(set(results[IPS])),
                    DF_DOMAINS:sorted(set(results[DF_DOMAINS])),
                    DF_IPS: sorted(set(results[DF_IPS]))
                }

        link_seen = set()
        for link in results[LINKS]:
            x, y = link[PROTO], link[URL]
            if x+y in link_seen:
                continue
            link_seen.add(x+y)
            clean_results[LINKS].append(link)
        for link in results[DF_LINKS]:
            x, y = link[PROTO], link[URL]
            if x+y in link_seen:
                continue
            link_seen.add(x+y)
            clean_results[DF_LINKS].append(link)
        return clean_results

    def extract_all(self):
        lines = [i.strip() for i in self.content.splitlines() if len(i.strip()) > 5]
        link_seen = set()
        ip_seen = set()
        hashes_seen = set()
        domain_seen = set()
        results = {
                    HASHES:[],
                    DOMAINS:[],
                    LINKS:[],
                    IPS: [],
                    DF_DOMAINS: [],
                    DF_LINKS: [],
                    DF_IPS: [], 
                }

        for line in lines:
            results = self.get_host_info_update(line, results, ip_seen, domain_seen)
            uinfos = self.extract_link(line)
            if len(uinfos[LINKS]) > 0:
                g = [i for i in uinfos[LINKS] if i[URL] not in link_seen]
                results[LINKS] = results[LINKS] + g
                link_seen |= set([i[URL] for i in g])
            
            if len(uinfos[DF_LINKS]) > 0:
                g = [i for i in uinfos[DF_LINKS] if i[URL] not in link_seen]
                results[DF_LINKS] = results[DF_LINKS] + g
                link_seen |= set([i[URL] for i in g])

            hinfo = self.extract_hash(line)
            if len(hinfo[HASHES]) > 0:
                g = [i for i in hinfo[HASHES] if i not in hashes_seen]
                results[HASHES] = results[HASHES] + g
                hashes_seen |= set(g)

        # update urls and such from the urls
        for link in results[LINKS]:
            _link = link[URL].replace('[.', '.').replace('.]', '.')
            uri = link[PROTO] + '://' + _link
            domain = urllib2.urlparse.urlsplit(uri).netloc
            # print "uri: %s domain: %s"% (uri, domain)
            if possible_domain(domain):
                results[DOMAINS].append(domain)
            results = self.get_host_info_update(link[URL], results, ip_seen, domain_seen)
        
        for link in results[DF_LINKS]:
            _link = link[URL].replace('[.', '.').replace('.]', '.')
            uri = link[PROTO] + '://' + _link
            domain = urllib2.urlparse.urlsplit(uri).netloc
            # print "uri: %s domain: %s"% (uri, domain)
            if possible_domain(domain):
                results[DF_DOMAINS].append(domain)
            results = self.get_host_info_update(link[URL], results, ip_seen, domain_seen)
        
        clean_results = {
                    HASHES:sorted(set(results[HASHES])),
                    LINKS:[],
                    DOMAINS:sorted(set(results[DOMAINS])),
                    IPS: sorted(set(results[IPS])),
                    DF_LINKS:[],
                    DF_DOMAINS:sorted(set(results[DF_DOMAINS])),
                    DF_IPS: sorted(set(results[DF_IPS]))
                }

        link_seen = set()
        for link in results[LINKS]:
            x, y = link[PROTO], link[URL]
            if x+y in link_seen:
                continue
            link_seen.add(x+y)
            clean_results[LINKS].append(link)
        
        link_seen = set()
        for link in results[DF_LINKS]:
            x, y = link[PROTO], link[URL]
            if x+y in link_seen:
                continue
            link_seen.add(x+y)
            clean_results[DF_LINKS].append(link)
        return clean_results
