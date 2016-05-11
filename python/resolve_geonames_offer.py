from optparse import OptionParser
from pyspark import SparkContext
from dictionaries import D
from geoname_extractor import processDoc
import ProbabilisticER
import json
import codecs

"""
RUN AS:
spark-submit --master local[*]    --executor-memory=8g     --driver-memory=8g \
--py-files lib/python-lib.zip main.py  /tmp/geonames/input/input.jl /tmp/geonames/geo-out \
/tmp/geonames/output/prior_dict.json 3 /tmp/geonames/output/state_dict.json /tmp/geonames/output/all_city_dict.json \
/tmp/geonames/output/city_faerie.json /tmp/geonames/output/state_faerie.json /tmp/geonames/output/all_dict_faerie.json \
 /tmp/geonames/output/tagging_dict.json /tmp/geonames/output/config.json
"""


us_states_names_readable = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida",
    "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma",
    "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

us_states_codes = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA",
    "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]


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


def get_only_city_name(x):
        values = x.split(" ")
        if len(values) > 1 and len(values[len(values) - 1]) == 2 and values[len(values) - 1].upper() != 'DC':
            return " ".join(values[0:len(values) - 1])
        return x


def get_state_from_city(x):
    values = x.split(" ")
    if len(values) > 1 and len(values[len(values) - 1]) == 2 and values[len(values) - 1].upper() != 'DC':
        short_state = values[len(values) - 1].upper()
        try:
            idx = us_states_codes.index(short_state)
            return us_states_names_readable[idx]
        except ValueError:
            return ''
    return ''


def create_input_geonames(line):
    result = []

    line = json.loads(line)

    fo = get_value_json('availableAtOrFrom.address', line)

    if fo != '':
        json_x = json.loads(fo)

        json_l = []
        if isinstance(json_x, dict):
            json_l.append(json_x)
        elif isinstance(json_x, list):
            json_l = json_x

        for x in json_l:
            # print x
            out = dict()
            out['country'] = get_value_json('addressCountry', x)

            city = get_value_json('addressLocality', x)
            out['locality'] = get_only_city_name(city)

            region = get_value_json('addressRegion', x)
            # out['region'] = region
            state = get_state_from_city(city)

            if region.strip() == '':
                if state.strip() != '':
                    out['region'] = state
                else:
                    out['region'] = ''
            else:
                out['region'] = region

            if out['country'].strip() == '' and out['region'].strip() == '' and out['locality'].strip() == '':
                print line['uri']
            else:
                out['uri'] = line['uri']

        result.append(out)
    return result

def create_address_object(geo):
    address = {}
    top_match = geo['matches'][0]
    address['addressLocality'] = top_match['value']['city']
    address['addressRegion'] = top_match['value']['state']
    address['addressCountry'] = top_match['value']['country']
    address['uri'] = geo['uri'] + "/address"
    address['createdBy'] = 'dig-geonames'
    return address


def merge_offers_with_geonames(x):
    try:
        offer = x[0]
        geo = x[1]
        if offer:
            if geo:
                # get score of the top match
                score = float(geo['matches'][0]['score'])
                if score >= 0.4502:
                    address = create_address_object(geo)
                    offer_address = get_value_json('availableAtOrFrom.address', offer)
                    if offer_address != '':
                        # del offer['availableAtOrFrom']['address']
                        offer['availableAtOrFrom']['address'].append(address)
                    else:
                        a_list = list()
                        a_list.append(address)
                        offer['availableAtOrFrom']['address'] = a_list
    except:
        print offer
        print geo

    return offer

if __name__ == "__main__":
    sc = SparkContext(appName="DIG-EntityResolution")

    parser = OptionParser()

    (c_options, args) = parser.parse_args()

    input_path = args[0]
    output_path = args[1]
    prior_dict_file = args[2]
    topk = int(args[3])
    state_dict_path = args[4]
    all_city_path = args[5]
    city_faerie = args[6]
    state_faerie = args[7]
    all_faerie = args[8]
    tagging_dict_file = args[9]
    ERconfig = args[10]

    # input_rdd = sc.textFile(input_path)
    input_address = sc.sequenceFile(input_path)
    input_address = sc.sequenceFile(input_path).mapValues(lambda x: json.loads(x))

    # dictc = D(sc, state_dict_path, all_city_path, city_faerie, state_faerie, all_faerie, prior_dict_file,tagging_dict_file)
    #
    # d = sc.broadcast(dictc)
    #
    # EV = ProbabilisticER.initializeRecordLinkage(json.load(codecs.open(ERconfig)))
    #
    # EV_b = sc.broadcast(EV)
    # input_rdd = input_address.flatMapValues(create_input_geonames)
    # resolved_geonames = input_rdd.mapValues(lambda x:processDoc(x, d)).mapValues(lambda x: ProbabilisticER.scoreCandidates(EV_b.value, x, d.value.priorDicts,
    #                                                                                  d.value.taggingDicts, topk, 'raw'))
    # resolved_geonames.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(output_path)
    resolved_geonames = sc.sequenceFile(output_path).mapValues(lambda x: json.loads(x))

    results = input_address.join(resolved_geonames).mapValues(lambda x: merge_offers_with_geonames(x))
    results.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(output_path + "-resolved")
