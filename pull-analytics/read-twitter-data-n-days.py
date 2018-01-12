from pymongo import MongoClient
import json
import argparse
import logging
import sys
from datetime import datetime, timedelta
from fiery_snap.impl.util.domain_tlds import filter_domain


logging.getLogger().setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s - %(name)s] %(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)

HASHES = 'hashes'
DOMAINS = 'domains'
EMAILS = 'emails'
URLS = 'urls'
PURLS = 'pot_urls'
IPS = 'ips'
CONTENT_ARTIFACTS = 'content_artifacts'

DF_DOMAINS = 'defanged_domains'
DF_EMAILS = 'defanged_emails'
DF_URLS = 'defanged_links'
DF_IPS = 'defanged_ips'

REPORT_ITEMS = [HASHES, DOMAINS, EMAILS, URLS, PURLS, IPS, CONTENT_ARTIFACTS]

DBNAME = 'twitter-feeds'
PROCESS_COLNAME = 'processed-tweets'
RAW_COLNAME = 'raw-tweets'

parser = argparse.ArgumentParser(
                      description='Read all the documents from the twitter feed document.')

parser.add_argument('-verbose', default=False, action="store_true",
                    help='verbose output')

parser.add_argument('-host', type=str, default='127.0.0.1',
                    help='mongo host')

parser.add_argument('-port', type=int, default='27017',
                    help='mongo port')


parser.add_argument('-processed', default=False, action='store_true',
                    help='read from processed from the default DB and Collection')

parser.add_argument('-colname', type=str, default=None,
                    help='collection name')

parser.add_argument('-dbname', type=str, default=None,
                    help='dbname name')

parser.add_argument('-outfile', type=str, default="data.json",
                    help='file to dump json')

parser.add_argument('-query', type=str, default="{}",
                    help='db query')

parser.add_argument('-days', type=int, default=0,
                    help='number of days to look back (make -1 to disable)')

parser.add_argument('-content', default=False, action='store_true',
                    help='print tweet before the other stuff')

parser.add_argument('-use_twitter_timestamp', default=False, action='store_true',
                    help='query N days back based on message timestamp instead of ingested date')

# options for processed twitter data
parser.add_argument('-all', default=False, action='store_true',
                    help='read information from extracted Twitter information')


for i in REPORT_ITEMS:
    parser.add_argument('-{}'.format(i), default=False, action='store_true',
                        help='include {}'.format(i))




def read_collection(host, port, dbname, colname, query={}):
    logging.info("Connecting to mongodb://%s:%d"%(host, port))
    client = MongoClient('mongodb://%s:%s/'%(host, port))
    db = client[dbname]
    col = db[colname]
    # read all objects
    logging.info("Querying %s[%s]: %s"%(dbname, colname, json.dumps(query)))
    num_posts = 0
    cur = col.find(query)
    results = []
    for i in cur:
        if '_id' in i:
            i['_id'] = str(i['_id'])
        results.append(i)
        if len(results) % 100 == 0:
            logging.info("Read %d records from %s[%s] (still working) ..."%(len(results), dbname, colname))
    logging.info("Found %d records in %s[%s]"%(len(results), dbname, colname))
    return results

def update_with_linked_content(record, tm_id, reports):
    r = record
    if 'linked_content' in r:
        for x in r['linked_content']:
            c = x[CONTENT_ARTIFACTS] if CONTENT_ARTIFACTS in x else None
            if c is None:
                continue
            if args.hashes:
                reports[tm_id][HASHES] = reports[tm_id][HASHES] + \
                                         c.get(HASHES, [])
            if args.domains:
                reports[tm_id][DOMAINS] = reports[tm_id][DOMAINS] + \
                                            c.get(DOMAINS, [])
            if args.urls:
                reports[tm_id][URLS] = reports[tm_id][URLS] + \
                                         c.get(URLS, [])
            if args.ips:
                reports[tm_id][IPS] = reports[tm_id][IPS] + \
                                        c.get(IPS, [])

def update_with_defanged_linked_content(record, tm_id, reports):
    r = record
    if 'linked_content' in r:
        for x in r['linked_content']:
            c = x[CONTENT_ARTIFACTS] if CONTENT_ARTIFACTS in x else None
            if c is None:
                continue
            if args.hashes:
                reports[tm_id][HASHES] = reports[tm_id][HASHES] + \
                                         c.get(HASHES, [])
            if args.domains:
                reports[tm_id][DOMAINS] = reports[tm_id][DOMAINS] + \
                                            c.get(DF_DOMAINS, [])
            if args.urls:
                reports[tm_id][URLS] = reports[tm_id][URLS] + \
                                         c.get(DF_URLS, [])
            if args.ips:
                reports[tm_id][IPS] = reports[tm_id][IPS] + \
                                        c.get(DF_IPS, [])

def filter_twitter_urls(record, tm_id, reports):
    reports[tm_id][URLS] = sorted([i for i in reports[tm_id][URLS] if i.strip().find('t.co/') != 0])


def update_with_entities_content(record, tm_id, reports):
    r = record
    e = r['entities']
    reports[tm_id] = dict([(i, {}) for i in items])
    reports[tm_id]['content'] = r['content']
    reports[tm_id]['user'] = r['user']
    reports[tm_id]['obtained_timestamp'] = r.get('obtained_timestamp', [])
    reports[tm_id]['timestamp'] = r.get('timestamp', [])
    reports[tm_id][HASHES] = e.get(HASHES, [])
    reports[tm_id][DOMAINS] = e.get(DOMAINS, [])
    reports[tm_id][URLS] = e.get(URLS, [])
    reports[tm_id][URLS] = reports[tm_id][URLS] + e.get(PURLS, [])
    reports[tm_id][IPS] = e.get(IPS, [])
    reports[tm_id][EMAILS] = e.get(EMAILS, [])

    update_with_defanged_linked_content(record, tm_id, reports)
    filter_twitter_urls(record, tm_id, reports)

    unique_sort = [HASHES, DOMAINS, URLS, IPS, EMAILS]
    for k in unique_sort:
        if k == DOMAINS:
            d = [i for i in sorted(set(reports[tm_id][k])) if not filter_domain(i)]
            reports[tm_id][k] = d
        else:
            reports[tm_id][k] = sorted(set(reports[tm_id][k]))

    for k, v in reports[tm_id].items():
        if k == 'user':
            continue
        user = reports[tm_id]['user']
        reports_by_user[user][k] = v

def update_with_defanged_entities_content(record, tm_id, reports):
    r = record
    e = r['defanged_entities']
    reports[tm_id] = dict([(i, {}) for i in items])
    reports[tm_id]['content'] = r['content']
    reports[tm_id]['user'] = r['user']
    reports[tm_id]['obtained_timestamp'] = r.get('obtained_timestamp', [])
    reports[tm_id]['timestamp'] = r.get('timestamp', [])
    e2 = r['entities']
    reports[tm_id][HASHES] = e2.get(HASHES, [])
    reports[tm_id][DOMAINS] = e.get(DOMAINS, []) + e2.get(DOMAINS, [])
    reports[tm_id][URLS] = e.get(URLS, [])
    reports[tm_id][URLS] = reports[tm_id][URLS] + e.get(PURLS, []) + e2.get(PURLS, [])
    reports[tm_id][IPS] = e.get(IPS, []) + e2.get(IPS, [])
    reports[tm_id][EMAILS] = e.get(EMAILS, []) + e2.get(EMAILS, [])
    filter_twitter_urls(record, tm_id, reports)

    unique_sort = [DOMAINS, URLS, IPS, EMAILS]
    for k in unique_sort:
        if k == DOMAINS:
            # d = [i for i in sorted(set(reports[tm_id][k])) if not filter_domain(i)]
            # reports[tm_id][k] = d
            reports[tm_id][k] = sorted(set(reports[tm_id][k]))
        else:
            reports[tm_id][k] = sorted(set(reports[tm_id][k]))



if __name__ == "__main__":
    args = parser.parse_args()
    print args.query
    query = json.loads(args.query)

    # add the days to the query
    if args.days > -1:
        start = datetime.utcnow()
        end = start - timedelta(days=args.days)
        ts_q = {'$gte': end.strftime("%Y-%m-%d %H:%M:%S"),
                '$lte': start.strftime("%Y-%m-%d %H:%M:%S")}
        if args.use_twitter_timestamp:
            query['timestamp'] = ts_q
        else:
            query['obtained_timestamp'] = ts_q

    results = {}
    if args.processed:
        results = read_collection(args.host, args.port,
                                  DBNAME, PROCESS_COLNAME, query)
    elif args.dbname is not None and args.colname is not None:
        results = read_collection(args.host, args.port,
                                  args.dbname, args.colname, query)
    # create report
    items = [HASHES, DOMAINS, EMAILS, URLS, IPS]
    reports = {}
    reports_by_user = {}
    for r in results:
        tm_id = r['tm_id']
        # update_with_entities_content(r, tm_id, reports)
        update_with_defanged_entities_content(r, tm_id, reports)
        # update_with_linked_content(r, tm_id, reports)

        user = reports[tm_id]['user']
        reports_by_user[user] = {}
        for k, v in reports[tm_id].items():
            if k == 'user':
                continue
            reports_by_user[user][k] = v

    for user, r in reports_by_user.items():
        timestamp = r['timestamp']
        s = u"".encode('utf-8')
        add_new_line = lambda x: s + '\n'.encode('utf-8')
        print_s = False
        if args.content:
            s = u"{} [{}] tweet: ".format(user, timestamp).encode('utf-8') + repr(r['content'].encode('utf-8'))
            # print (s.replace('\n', ' '))
        if (args.all or args.hashes) and len(r[HASHES]) > 0:
            s = add_new_line(s) + u"{} [{}] hashes: {}".format(user, timestamp, u", ".join(r[HASHES])).encode('utf-8')
            print_s = True
        if (args.all or args.domains) and len(r[DOMAINS]) > 0:
            s = add_new_line(s) + u"{} [{}] domains: {}".format(user, timestamp, u", ".join(r[DOMAINS]))
            print_s = True
        if (args.all or args.urls) and len(r[URLS]) > 0:
            s = add_new_line(s) + u"{} [{}] urls: {}".format(user, timestamp, u", ".join(r[URLS])).encode('utf-8')
            # print(s)
        if (args.all or args.ips) and len(r[IPS]) > 0:
            s = add_new_line(s) + u"{} [{}] ips: {}".format(user, timestamp, u", ".join(r[IPS])).encode('utf-8')
            print_s = True
        if (args.all or args.emails) and len(r[EMAILS]) > 0:
            s = add_new_line(s) + u"{} [{}] emails: {}".format(user, timestamp, u", ".join(r[EMAILS]))
            print_s = True

        if print_s or args.verbose:
            print(add_new_line(s))

    if args.outfile:
        with open(args.outfile, 'wt') as out:
            res = json.dumps(reports, sort_keys=True, indent=4, separators=(',', ': '))
            out.write(res)
            logging.info("Wrote (%d bytes) data to file: %s"%(len(res), args.outfile))
