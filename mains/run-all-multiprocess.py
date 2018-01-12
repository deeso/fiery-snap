from fiery_snap.parse import ConfigParser, OUTPUTS, INPUTS, \
                            PROCESSORS, CLASS_MAPS, TAGS, OSINT
import traceback
import argparse
import sys
import os
import logging
import multiprocessing
import time

logging.getLogger().setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s - %(name)s] %(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)


CMD_DESC = 'Meta-flow framework for pipelining data extraction, transformation, and forwarding.'
parser = argparse.ArgumentParser(description=CMD_DESC)
parser.add_argument('-config', type=str, default=None,
                    help='config file containing a decription of io and flows')

parser.add_argument('-reset', default=False, action='store_true',
                    help='reset all queues and dbs')

parser.add_argument('-name', type=str, default='ALL',
                    help='name of component to run (default is all)')



def extract_components(results):
    components = {}
    # all the processors
    if PROCESSORS in results and results[PROCESSORS] is not None:
        for name, item in results[PROCESSORS].items():
            components[name] = item
    if OUTPUTS in results and results[OUTPUTS] is not None:
        for name, item in results[OUTPUTS].items():
            components[name] = item
    if INPUTS in results and results[INPUTS] is not None:
        for name, item in results[INPUTS].items():
            components[name] = item
    return components


def reset_components(components):
    for name, item in components.items():
        logging.info("Reseting consumer queues: %s" % name)
        item.reset_all()
        qs_state = item.is_empty()
        for qname, is_empty in qs_state.items():
            logging.info("%s queue %s is empty: %s"%(name, qname, is_empty))
        logging.info("Reseting %s queues complete" % name)
    logging.info("Reseting components completed")


def run_component(component_name, config_file):
    results = ConfigParser.parse_components(config_file)
    components = extract_components(results)
    item = components[component_name]

    logging.debug("Starting consumer type: %s" % (component_name))
    item.periodic_consume(2.0)

    while True:
        time.sleep(10)


logging.error("[XXXXXXX] Starting")
if __name__ == '__main__':
    args = parser.parse_args()
    if args.config is None:
        parser.print_help()
        sys.exit(1)

    try:
        os.stat(args.config)
    except:
        logging.error("[X] Error file does not exist")
        sys.exit(1)

    component_procs = {}
    
    results = ConfigParser.parse_components(args.config)
    components = extract_components(results)
    if args.reset:
        reset_components(components)

    cname = None
    if args.name != "ALL":
        cname = args.name
        logging.info("Creating process for  %s"%cname)
        component_procs[cname] = multiprocessing.Process(name=cname, target=run_component, args=(cname, args.config))
        logging.info("Starting %s"%cname)
        component_procs[cname].start()

    else:
        # start all the components in separate procs
        for name, item in components.items():
            logging.info("Creating process for  %s"%name)
            component_procs[name] = multiprocessing.Process(name=name, target=run_component, args=(name, args.config))

        # start the component_procs
        for n,p in component_procs.items():
            logging.info("Starting %s"%n)
            p.start()
            time.sleep(2)

    # loop until keyboard interrupt
    while True:
        try:
            time.sleep(10.0)
        except:
            break

    for n, p in component_procs.items():
        logging.info("Terminating %s"%n)
        try:
            p.terminate()
            time.sleep(0.1)
        except:
            logging.debug('Failed with the following exception:\n{}'.format(traceback.format_exc()))

    for n, p in component_procs.items():
        logging.info("Joing %s"%n)
        p.join()
