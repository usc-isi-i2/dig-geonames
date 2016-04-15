import codecs
import json
from optparse import OptionParser
from pyspark import SparkContext


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


def process_doc(line):
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

    parser = OptionParser()

    (c_options, args) = parser.parse_args()

    input_file = args[0]
    output_file = args[1]
    sc = SparkContext(appName="DIG-CREATE-INPUT-ENTITYRESOLUTION")

    input_rdd = sc.sequenceFile(input_file)
    print input_rdd.count()
    print input_rdd.first()[1]
    out_rdd = input_rdd.map(lambda x: process_doc(x[1]))

    out_rdd.map(lambda x: json.dumps(x)).saveAsTextFile(output_file)
