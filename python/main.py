from optparse import OptionParser
from pyspark import SparkContext
from dictionaries import D
from geoname_extractor import processDoc
from ProbabilisticER import recordLinkage
import json

# Given a path in json, return value if path, full path denoted by a separator,like '$'or '.',
#  (example address.name) exists, otherwise return ''
def get_value_json(path, doc, separator='.'):
    paths = path.strip().split(separator)
    for field in paths:
        if field in doc:
            doc = doc[field]
        else:
            return ''

    if type(doc) == dict or type(doc) == list:
        return json.dumps(doc)
    else:
        return doc


def create_input_geonames(line):
    out = {}
    line = json.loads(line)
    out['uri'] = line['uri']

    fo = get_value_json('hasFeatureCollection.place_postalAddress_feature', line)

    json_x = json.loads(fo)

    json_l = []
    if isinstance(json_x, dict):
        json_l.append(json_x)
    elif isinstance(json_x, list):
        json_l = json_x

    for x in json_l:
        print x
        out['country'] = get_value_json('featureObject.addressCountry.label', x)
        out['region'] = get_value_json('featureObject.addressRegion', x)
        out['locality'] = get_value_json('featureObject.addressLocality', x)

    return out


if __name__ == "__main__":
    sc = SparkContext(appName="DIG-EntityResolution")

    parser = OptionParser()

    (c_options, args) = parser.parse_args()

    input_path = args[0]
    output_path = args[1]
    prior_dict_file = args[2]
    topk = args[3]
    state_dict_path = args[4]
    all_city_path = args[5]
    city_faerie = args[6]
    state_faerie = args[7]
    all_faerie = args[8]
    tagging_dict_file = args[9]
    ERconfig = args[10]

    input_rdd = sc.textFile(input_path)

    dictc = D(sc, state_dict_path, all_city_path, city_faerie, state_faerie, all_faerie, prior_dict_file,tagging_dict_file)

    d = sc.broadcast(dictc)

    input_address = sc.sequenceFile(input_path)

    input_rdd = input_address.mapValues(create_input_geonames)

    results = input_rdd.map(lambda x:processDoc(x, d)).map(lambda x: recordLinkage(ERconfig, x, topk,
                                                                                   d.value.priorDicts,
                                                                                   d.value.taggingDicts))
