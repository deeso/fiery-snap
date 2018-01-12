from pymongo import MongoClient
import json
import argparse
import logging
import sys

logging.getLogger().setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s - %(name)s] %(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)

DBNAME = 'twitter-feeds'
PROCESS_COLNAME = 'processed-tweets'
RAW_COLNAME = 'raw-tweets'

parser = argparse.ArgumentParser(
                      description='Read all the documents from the twitter feed document.')

parser.add_argument('-host', type=str, default='127.0.0.1',
                    help='mongo host')

parser.add_argument('-port', type=int, default='27017',
                    help='mongo port')


parser.add_argument('-processed', default=False, action='store_true',
                    help='read from processed')

parser.add_argument('-raw', default=False, action='store_true',
                    help='read from raw')

parser.add_argument('-colname', type=str, default=None,
                    help='collection name')

parser.add_argument('-dbname', type=str, default=None,
                    help='dbname name')

parser.add_argument('-outfile', type=str, default="data.json",
                    help='file to dump json')

parser.add_argument('-query', type=str, default="{}",
                    help='db query')

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

if __name__ == "__main__":
    args = parser.parse_args()
    print args.query
    query = json.loads(args.query)
    results = {}
    if args.raw:
        results[DBNAME] = {}
        results[DBNAME][RAW_COLNAME] = read_collection(args.host, args.port, DBNAME, RAW_COLNAME, query)

    if args.processed:
        if DBNAME not in results:
            results[DBNAME] = {}
        results[DBNAME][PROCESS_COLNAME] = read_collection(args.host, args.port, DBNAME, PROCESS_COLNAME, query)

    if args.dbname is not None and args.colname is not None:
        if args.dbname not in results:
            results[args.dbname] = {}
        results[args.dbname][args.colname] = read_collection(args.host, args.port, args.dbname, args.colname, query)

    if args.outfile:
        with open(args.outfile, 'wt') as out:
            res = json.dumps(results, sort_keys=True, indent=4, separators=(',', ': '))
            out.write(res)
            logging.info("Wrote (%d bytes) data to file: %s"%(len(res), args.outfile))
