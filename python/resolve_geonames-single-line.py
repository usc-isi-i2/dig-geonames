import sys
sys.path.insert(0, '/Users/majid/EntityResolution/')
from optparse import OptionParser
from pyspark import SparkContext, StorageLevel
from dictionaries import V
from geoname_extractor import processDoc
from ProbabilisticER import scoreCandidates, initializeRecordLinkage
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
'''
~/dig-entity-resolution/prior_dict.json ~/dig-entity-resolution/state_dict.json ~/dig-entity-resolution/all_city_dict.json \
~/dig-entity-resolution/city_faerie.json ~/dig-entity-resolution/state_faerie.json ~/dig-entity-resolution/all_dict_faerie.json \
 ~/dig-entity-resolution/tagging_dict.json ~/dig-entity-resolution/config.json 3
'''

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

class geoname_resolver:
    # Given a path in json, return value if path, full path denoted by a separator,like '$'or '.',
    #  (example address.name) exists, otherwise return ''
    def get_value_json(self, path, doc, separator='.'):
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


    def get_only_city_name(self, x):
            values = x.split(" ")
            if len(values) > 1 and len(values[len(values) - 1]) == 2 and (values[len(values) - 1].upper() != 'DC' or values[len(values) - 1].upper() != 'UK' or values[len(values) - 1].upper() != 'US'):
                return " ".join(values[0:len(values) - 1])
            return x


    def get_state_from_city(self, x):
        values = x.split(" ")
        if len(values) > 1 and len(values[len(values) - 1]) == 2 and (values[len(values) - 1].upper() != 'DC' or values[len(values) - 1].upper() != 'UK' or values[len(values) - 1].upper() != 'US'):
            short_state = values[len(values) - 1].upper()
            try:
                idx = us_states_codes.index(short_state)
                return us_states_names_readable[idx]
            except ValueError:
                return ''
        return ''


    def create_input_geonames(self, line):
        result = []

        # line = json.loads(line)

        if line:
            json_x = line

            json_l = []
            if isinstance(json_x, dict):
                json_l.append(json_x)
            elif isinstance(json_x, list):
                json_l = json_x

            for x in json_l:
                # print x
                out = dict()
                out['country'] = self.get_value_json('addressCountry', x)

                city = self.get_value_json('addressLocality', x)
                out['locality'] = self.get_only_city_name(city)

                region = self.get_value_json('addressRegion', x)
                # out['region'] = region
                state = self.get_state_from_city(city)

                if region.strip() == '':
                    if state.strip() != '':
                        out['region'] = state
                    else:
                        out['region'] = ''
                else:
                    out['region'] = region

                if out['country'].strip() == '' and out['region'].strip() == '' and out['locality'].strip() == '':
                    print line['uri']

                if out['locality'].strip() == '':
                    n = ''
                    if out['region'].strip() != '':
                        n = out['region']
                    if out['country'].strip() != '':
                        if n == '':
                            n = out['country']
                        else:
                            n += ',' + out['country']

                    out['locality'] = n

                out['uri'] = line['uri']

            result.append(out)
        return result


    def getAddressName(self, x):
        name = ''
        city = self.get_value_json('addressLocality', x)
        state = self.get_value_json('addressRegion', x)
        country = self.get_value_json('addressCountry', x)

        if city != '':
            name += city + ','

        if state != '':
            name += state + ','

        if country != '':
            name += country
        if name != '':
            if name[len(name)-1] == ',':
                name = name[:len(name)-1]
        return name


    def create_address_object(self, geo, x, d):
        address = {}
        try:
            extractor_output = self.getAddressName(x)
            key = ''
            top_match = geo['matches'][0]
            address['addressLocality'] = top_match['value']['city']
            address['addressRegion'] = top_match['value']['state']
            address['addressCountry'] = top_match['value']['country']
            address['uri'] = x['uri']
            address['createdBy'] = 'dig-geonames'
            address['a'] = "http://schema.org/PostalAddress"
            address['@context'] = "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/" \
                                  "development/versions/3.0/karma/karma-context.json"
            key += address['addressLocality'] + ":" + address['addressRegion'] + ":" + address['addressCountry']
            city_dict = d.value.all_city_dict
            geo_uri = top_match['uri']
            address['extractorOutput'] = extractor_output
            geoname_city = city_dict[geo_uri]
            if 'longitude' in geoname_city and 'latitude' in geoname_city:
                address['geo'] = dict()
                address['geo']['longitude'] = geoname_city['longitude']
                address['geo']['latitude'] = geoname_city['latitude']
                address['geo']['a'] = 'http://schema.org/GeoCoordinates'
                key += ":" + geoname_city['longitude'] + ":" + geoname_city['latitude']
            else:
                print geo_uri

            address['key'] = key

        except Exception, e:
            print e

        return address


    def create_pa_with_name(self, x):
        out = dict()
        out['name'] = self.getAddressName(x)
        out['uri'] = x['uri']
        out['a'] = "http://schema.org/PostalAddress"
        out['@context'] = "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/" \
                          "versions/3.0/karma/karma-context.json"
        return out



    def merge_postal_addresses(self, x, d):
        old = x[0]
        new = x[1]

        if old:
            if new:
                if len(new['matches']) >= 1:
                    address = self.create_address_object(new, old, d)
                    old = address
                    return old

        old = self.create_pa_with_name(old)
        return old


    def merge_offers_with_geonames(self, x, d):
        try:
            offer = x[0]
            geo = x[1]
            if offer:
                if geo:
                    # get score of the top match
                    score = float(geo['matches'][0]['score'])
                    if score >= 0.4502:
                        address = self.create_address_object(geo, d)
                        offer_address = self.get_value_json('availableAtOrFrom.address', offer)
                        if offer_address != '':
                            offer['availableAtOrFrom']['address'].append(address)

                            addresses = list()

                            for a in offer['availableAtOrFrom']['address']:
                                if 'createdBy' in a:
                                    addresses.append(a)

                            offer['availableAtOrFrom']['address'] = addresses

                        else:
                            a_list = list()
                            a_list.append(address)
                            offer['availableAtOrFrom']['address'] = a_list
        except:
            print offer
            print geo

        return offer


    def filter_broken_addresses(self, x):
        if x:
            if self.get_value_json('addressLocality', x) != '' or self.get_value_json('name', x) != '':
                return True

        return False


    def reformatDocs(self, jobj, all_city_dict):
        # print(jobj)
        candidates = []
        for uri in jobj['entities'].keys():
            geoname = all_city_dict[uri]
            city = geoname['name']
            state = geoname['state']
            country = geoname['country']
            candidates.append({'id': uri,
                               'value': {'city': city if type(city) is list else [city],
                                         'state': state if type(state) is list else [state],
                                         'country': country if type(country) is list else [country]}})
        return {'document': jobj['document'], 'entities': candidates, 'processtime': jobj['processtime']}

    def __init__(self,state_dict_path, all_city_path, city_faerie,
                 state_faerie, all_faerie, prior_dict_file, tagging_dict_file, ERconfig):
        self.dictc = V(state_dict_path, all_city_path, city_faerie,
                  state_faerie, all_faerie, prior_dict_file, tagging_dict_file)
        self.EV = initializeRecordLinkage(json.load(codecs.open(ERconfig)))


    def resolve(self, inputline):
        # sc = SparkContext(appName="DIG-EntityResolution")

        # parser = OptionParser()

        # parser.add_option("-r", "--rungeonames", dest="rungeonames", default=False,
        #                   action="store_true")
        # parser.add_option("-l", "--loadgeonames", dest="loadgeonames", default=False,
        #                   action="store_true")

        # (c_options, args) = parser.parse_args()

        # rungeonames = c_options.rungeonames
        # loadgeonames = c_options.loadgeonames
        processed_doc = processDoc(inputline, self.dictc)
        # print(processed_doc)
        reformatted_docs = self.reformatDocs(processed_doc, self.dictc.value.all_city_dict)
        resolved_geonames = scoreCandidates(self.EV, reformatted_docs, self.dictc.value.priorDicts, self.dictc.value.taggingDicts, topk, 'raw')
        print(resolved_geonames)

        # EV_b = sc.broadcast(EV)
        #
        # input_reduced = sc.sequenceFile(input_path).mapValues(lambda x: json.loads(x))
        # input_address = input_reduced.filter(lambda x: x[1]['a'] == 'http://schema.org/PostalAddress')
        #
        # # initialise as None to avoid running into errors
        # resolved_geonames = None
        # if rungeonames:
        #     input_rdd = input_address.flatMapValues(create_input_geonames)
        #     # resolved_geonames = input_rdd.mapValues(lambda x: processDoc(x, d)).\
        #     #     mapValues(lambda x: ProbabilisticER.scoreCandidates(EV_b.value, x, d.value.priorDicts,
        #     #                                                         d.value.taggingDicts, topk, 'raw'))
        #     resolved_geonames = input_rdd.mapValues(lambda x:processDoc(x, d)).\
        #         mapValues(lambda x: self.reformatDocs(x, d.value.all_city_dict)).\
        #         mapValues(lambda x: ProbabilisticER.scoreCandidates
        #         (EV_b.value, x, d.value.priorDicts, d.value.taggingDicts, topk, 'raw'))
        #
        #     print resolved_geonames.first()
        #     resolved_geonames.persist(StorageLevel.MEMORY_AND_DISK)
        #     resolved_geonames.setName('RESOLVED_GEONAMES')
        #     resolved_geonames.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(output_path)
        # elif loadgeonames:
        #     resolved_geonames = sc.sequenceFile(output_path).mapValues(lambda x: json.loads(x))
        #
        # if resolved_geonames:
        #     results = input_address.join(resolved_geonames).mapValues(lambda x: merge_postal_addresses(x, d))
        #     print results.first()
        #     # results_filtered = results.filter(lambda x: filter_broken_addresses(x[1]))
        #     results.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(output_path + "-resolved")
        # else:
        #     print "resolved_geonames is none"


if __name__ == "__main__":
    parser = OptionParser()
    (c_options, args) = parser.parse_args()

    prior_dict_file = args[0]
    state_dict_path = args[1]
    all_city_path = args[2]
    city_faerie = args[3]
    state_faerie = args[4]
    all_faerie = args[5]
    tagging_dict_file = args[6]
    ERconfig = args[7]
    topk = int(args[8])

    geores = geoname_resolver(state_dict_path, all_city_path, city_faerie,
                  state_faerie, all_faerie, prior_dict_file, tagging_dict_file, ERconfig)
    geores.resolve({'uri':'1',
                    'locality':"san francisco",
                    'region':"california",
                    'country':"united states"})