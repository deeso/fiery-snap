import string
import random
from datetime import datetime
from email.utils import parsedate


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    # http://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable-in-python
    if isinstance(obj, datetime):
        return (obj - datetime.fromtimestamp(0)).total_seconds()
    raise TypeError("Type not serializable")


def undefang(domain):
    return domain.replace('[', '').replace(']', '')


def defang(domain):
    return domain.replace('.', '[.]')


def parsedate_to_datetime(data):
    dtuple = parsedate(data)
    return datetime(*dtuple[:6])


def generate_obj_name(obj):
    on = type(obj)
    hc = hash(obj)
    return "%s-%08x" % (on, hc)


def random_str(length=5, chars="ULD"):
    choices = ''
    if 'U' in chars:
        choices = choices + string.ascii_uppercase
    if 'L' in chars:
        choices = choices + string.ascii_lowercase
    if 'D' in chars:
        choices = choices + string.digits
    return ''.join(random.choice(choices) for _ in range(length))


def flatten_dictionary(ckey='', dict_obj={}):
    results = {}
    new_key_fmt = "{}{}"
    if len(ckey) > 0:
        new_key_fmt = "{}.{}"

    for k, v in dict_obj.items():
        nk = new_key_fmt.format(ckey, k)
        if isinstance(v, dict):
            results.update(flatten_dictionary(nk, v))

        elif isinstance(v, list) and len(v) > 0:
            r = []
            for lv in v:
                nv = lv
                if isinstance(v, dict):
                    nv = flatten_dictionary(nk, v)
                r.append(nv)
            results[nk] = r

        else:
            results[nk] = v
    return results
