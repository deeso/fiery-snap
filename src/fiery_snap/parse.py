import regex
import toml
from . import CLASS_MAPS

INPUTS = 'inputs'
OUTPUTS = 'outputs'
FLOWS = 'flows'
XTRACTS = 'extractors'
XFORMS = 'transforms'
PROCESSORS = 'processors'
OSINT = 'osint'
TAGS = 'tags'
KEYWORDS = 'note_keywords'
KEYWORDS_CI = 'note_keywords_caseinsensitive'



class ConfigParser(object):
    @classmethod
    def parse(cls, config_file, class_mapper=CLASS_MAPS):
        flows_dict = toml_dict.get(FLOWS, None)
        results = cls.parse_components(config_file, class_mapper)
        flows = cls.parse_flows(flows_dict, class_mapper, **results)
        return flows

    @classmethod
    def parse_components(cls, config_file, class_mapper=CLASS_MAPS):
        toml_dict = toml.load(open(config_file))
        return cls.parse_components_dict(toml_dict, class_mapper=class_mapper)

    @classmethod
    def parse_components_dict(cls, toml_dict, class_mapper=CLASS_MAPS):
        inputs_dict = toml_dict.get(INPUTS, None)
        outputs_dict = toml_dict.get(OUTPUTS, None)
        xforms_dict = toml_dict.get(XFORMS, None)
        xtracts_dict = toml_dict.get(XTRACTS, None)
        processors_dict = toml_dict.get(PROCESSORS, None)
        # if inputs_dict is None:
        #     raise Exception("Missing a %s block" % INPUTS)
        # if outputs_dict is None:
        #     raise Exception("Missing a %s block" % OUTPUTS)
        # if flows_dict is None:
        #     raise Exception("Missing a %s block" % FLOWS)
        # if xforms_dict is None:
        #     raise Exception("Missing a %s block" % XFORMS)
        # if xtracts_dict is None:
        #     raise Exception("Missing a %s block" % XTRACTS)
        # if processors_dict is None:
        #     raise Exception("Missing a %s block" % PROCESSORS)

        osint_dict = toml_dict.get(OSINT, None)

        regex_tags = []
        if TAGS in osint_dict:
            regex_tags = [[n, regex.compile(rv)] for n, rv in osint_dict[TAGS]]

        regex_keywords = []
        if KEYWORDS in osint_dict:
            regex_keywords = [[n, regex.compile(rv)] for n, rv in osint_dict[KEYWORDS]]
        regex_keywords_ci = osint_dict.get(KEYWORDS_CI, True)

        xforms = None
        xtracts = None
        processors = None
        inputs = None
        outputs = None
        if xforms_dict is not None:
            xforms = cls.parse_xtracts(xforms_dict, class_mapper)
        if xtracts_dict is not None:        
            xtracts = cls.parse_xtracts(xtracts_dict, class_mapper)
        if processors_dict is not None:
            processors = cls.parse_processors(processors_dict, class_mapper,
                                       xforms=xforms, xtracts=xtracts,
                                       regex_tags=regex_tags,
                                       regex_keywords=regex_keywords,
                                       regex_keywords_ci=regex_keywords_ci)
        if inputs_dict is not None:
            inputs = cls.parse_inputs(inputs_dict, class_mapper)
        if outputs_dict is not None:
            outputs = cls.parse_outputs(outputs_dict, class_mapper)
        return {
                    INPUTS: inputs, 
                    OUTPUTS: outputs, 
                    PROCESSORS: processors,
                    TAGS: regex_tags,
                    KEYWORDS: regex_keywords,
                    KEYWORDS_CI: regex_keywords_ci,
                    OSINT: osint_dict,
                    XFORMS: xforms,
                    XTRACTS: xtracts,
                }

    @classmethod
    def general_parse(cls, toml_dict, class_mapper, btype, **kargs):
        results = {}
        for name, block in toml_dict.items():
            t = block.get('type', None)
            if t is None and btype not in class_mapper:
                print block
                raise Exception('Missing %s parameter in %s.%s' %
                                ('type', btype, name))
            if t not in class_mapper and btype not in class_mapper:
                raise Exception('Unknown %s parameter in %s.%s' %
                                (str(t), btype, name))

            t = t if t is not None else btype
            klass = class_mapper.get(t)
            it = klass.parse(block, **kargs)
            results[name] = it
        return results

    @classmethod
    def parse_inputs(cls, toml_dict, class_mapper, **kargs):
        return cls.general_parse(toml_dict, class_mapper, INPUTS, **kargs)

    @classmethod
    def parse_outputs(cls, toml_dict, class_mapper, **kargs):
        return cls.general_parse(toml_dict, class_mapper, OUTPUTS, **kargs)

    @classmethod
    def parse_flows(cls, toml_dict, class_mapper, **kargs):
        results = {}
        for name, block in toml_dict.items():
            t = 'flow'
            # t = block.get('type', None)
            # if t is None and btype not in class_mapper:
            #     raise Exception('Missing %s parameter in %s.%s' %
            #                     ('type', btype, name))
            # if t not in class_mapper and btype not in class_mapper:
            #     raise Exception('Unknown %s parameter in %s.%s' %
            #                     (str(t), btype, name))
            # t = t if t is not None else btype
            klass = class_mapper.get(t)
            it = klass.parse(block, **kargs)
            results[name] = it
        return results

    @classmethod
    def parse_xtracts(cls, toml_dict, class_mapper, **kargs):
        return cls.general_parse(toml_dict, class_mapper, XTRACTS, **kargs)

    @classmethod
    def parse_xforms(cls, toml_dict, class_mapper, **kargs):
        return cls.general_parse(toml_dict, class_mapper, XFORMS, **kargs)

    @classmethod
    def parse_processors(cls, toml_dict, class_mapper, **kargs):
        return cls.general_parse(toml_dict, class_mapper, PROCESSORS, **kargs)
