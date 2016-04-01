import codecs
import json
from optparse import OptionParser


# Given a path in json, return value if path, full path denoted by . (example address.name) exists, otherwise return ''
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

if __name__ == "__main__":

    parser = OptionParser()

    (c_options, args) = parser.parse_args()

    input_file = args[0]
    output_file = args[1]

    f = codecs.open(input_file, 'r', 'utf-8')
    out_f = codecs.open(output_file, 'w', 'utf-8')

    lines = f.readlines()

    for line in lines:
        out = {}
        jo = json.loads(line)
        out['uri'] = jo['_id']

        fo = get_value_json('_source.hasFeatureCollection.place_postalAddress_feature', jo)

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

        out_f.write(json.dumps(out))
        out_f.write('\n')

out_f.close()
f.close()