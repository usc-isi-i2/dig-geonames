import codecs
import json
from optparse import OptionParser
import faerie
import math


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

def createGeonamesPriorDict(all_city_dict):
    pdict = {}
    for uri, val in all_city_dict.items():
        population = int(val['populationOfArea'])
        effective_population = population + (int(1e7) if val['snc'].split(',')[1].lower() == 'united states' else 0)
        prior = (1.0 - 1.0/math.log(effective_population + 2000))
        pdict.update({uri: prior})
    return json.dumps(pdict)


def createDict1(path):
    dicts = {}
    wholecities_dicts = {}
    all_cities_dict={}
    wholestates_dicts = {}
    for line in open(path):

        line = json.loads(line)
        if 'name' in line:
            city = line["name"]
            population = '0'
            if 'populationOfArea' in line:
                population = line['populationOfArea']
            longitude = ''
            latitude = ''
            if 'geo' in line:
                longitude = line['geo']['longitude']
                latitude = line['geo']['latitude']

            city_uri = line["uri"]
            country = line["address"]["addressCountry"]["name"]
            if type(country) != list:
                    country = [country]
            country_uri = line["address"]["addressCountry"]["sameAs"]
            if "addressRegion" in line["address"] and "name" in line["address"]["addressRegion"] and "sameAs" in line["address"]["addressRegion"]:
                state = line["address"]["addressRegion"]["name"]
                if type(state) != list:
                    state = [state]
                state_uri = line["address"]["addressRegion"]["sameAs"]
                wholestates_dicts[state_uri] = {}
                wholestates_dicts[state_uri]["name"] = state
                wholestates_dicts[state_uri]["country_uri"] = country_uri
            else:
                state = []
                state_uri = "N/A"
            try:
                stateDict = dicts[country_uri]["states"]
                try:
                    stateDict[state_uri]["cities"][city_uri] = {}
                    stateDict[state_uri]["cities"][city_uri]["name"] = city
                    stateDict[state_uri]["cities"][city_uri]["state"] = state
                    stateDict[state_uri]["cities"][city_uri]["country"] = country
                except KeyError:
                    stateDict[state_uri] = {"cities": {city_uri: {"name": city, "state": state, "country": country}},
                                                "name": state}
            except KeyError:
                dicts[country_uri] = {"states": {
                        state_uri: {"name": state, "cities": {city_uri: {"name": city, "state": state, "country": country}}}},
                                            "name": country}

            if state != '' and country != '':
                if int(population) >= 25000:
                    wholecities_dicts[city_uri] = {}
                    wholecities_dicts[city_uri]["name"] = city
                    wholecities_dicts[city_uri]["state"] = state
                    wholecities_dicts[city_uri]["country"] = country
                    wholecities_dicts[city_uri]['populationOfArea'] = population
                all_cities_dict[city_uri] = {}
                all_cities_dict[city_uri]['name'] = city
                all_cities_dict[city_uri]['state'] = state
                all_cities_dict[city_uri]['country'] = country
                all_cities_dict[city_uri]['populationOfArea'] = population
                if longitude!= '' and latitude != '':
                    all_cities_dict[city_uri]['longitude'] = longitude
                    all_cities_dict[city_uri]['latitude'] = latitude

    return wholecities_dicts, wholestates_dicts, dicts, all_cities_dict

def createDict2(all_dict, state_dict, city_dict):
    dicts = {}
    wholestates_dicts = faerie.readDictlist(state_dict, 2)
    wholecities_dicts = faerie.readDictlist(city_dict, 2)
    dicts["countries_dict"] = faerie.readDictlist(all_dict, 2)
    for country in all_dict:
        states = all_dict[country]["states"]
        dicts[country] = {}
        dicts[country]["states_dict"] = faerie.readDictlist(states, 2)
        for state in states:
            cities = states[state]["cities"]
            dicts[country][state] = {}
            dicts[country][state]["cities"] = faerie.readDictlist(cities, 2)
    return wholecities_dicts,wholestates_dicts, dicts


def create_tagging_dict(refPath):
    states = set()
    countries = set()
    citites = set()

    for line in open(refPath):
        jsonobj = json.loads(line)

        state = get_value_json('address.addressRegion.name', jsonobj).lower()
        if state != '':
            states.add(state)

        country = get_value_json('address.addressCountry.name', jsonobj).lower()
        if country != '':
            countries.add(country)

        if jsonobj['a'] == 'City':
            names=[]
            if 'name' in jsonobj:
                names_d = jsonobj['name']
                if isinstance(names_d, list):
                    names = names_d
                else:
                    names.append(names_d)

                for name in names:
                    citites.add(name.lower())
    # print citites
    return {'city': {x:0 for x in citites},
            'state': {x:0 for x in states},
            'country': {x:0 for x in countries}}


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-r", "--separator", dest="separator", type="string",
                      help="field separator", default="\t")

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options

    input_path = args[0]
    output_path = args[1]
    f1, f2, f3, f4 = createDict1(input_path)


    state_dict = codecs.open(output_path + "/state_dict.json", 'w')
    state_dict.write(json.dumps(f2))

    wcd, wsd, d = createDict2(f3, f2, f1)

    wcd_faerie = codecs.open(output_path +'/city_faerie.json', 'w')
    wcd_faerie.write(json.dumps(wcd))

    wsd_faerie = codecs.open(output_path + "/state_faerie.json", 'w')
    wsd_faerie.write(json.dumps(wsd))

    d_faerie = codecs.open(output_path + "/all_dict_faerie.json", 'w')
    d_faerie.write(json.dumps(d))

    all_city_dict = codecs.open(output_path + "/all_city_dict.json", 'w')
    all_city_dict.write(json.dumps(f4))

    prior_dict = codecs.open(output_path + "/prior_dict.json", 'w')
    prior = createGeonamesPriorDict(f4)
    prior_dict.write(prior)

    tagging_dict = codecs.open(output_path + "/tagging_dict.json", 'w')
    tagging_dict.write(json.dumps(create_tagging_dict(input_path)))
