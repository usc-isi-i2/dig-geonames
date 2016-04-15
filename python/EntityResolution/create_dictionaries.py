import codecs
import json
from optparse import OptionParser
import faerie
import tagging


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

def createGeonameDicts(refPath):
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
            print country
            countries.add(country)

        if jsonobj['a'] == 'City':
            names=[]
            if 'name' in jsonobj:
                names_d = jsonobj['name']
                if isinstance(names_d, list):
                    names = names_d
                elif isinstance(names_d, str):
                    names.append(names_d)

                for name in names:
                    citites.add(name.lower())
    print countries
    return {'city': {x:0 for x in citites},
            'state': {x:0 for x in states},
            'country': {x:0 for x in countries}}


def create_prior_dict(path):
    return json.dumps(createGeonameDicts(path))


def createDict1(path):
    dicts = {}
    wholecities_dicts = {}
    all_cities_dict={}
    wholestates_dicts = {}
    for line in open(path):

        line = json.loads(line)
        if 'name' in line:
            city = line["name"]
            population = line['populationOfArea']

            city_uri = line["uri"]
            try:
                state = line["address"]["addressRegion"]["name"]
                state_uri = line["address"]["addressRegion"]["sameAs"]
                country = line["address"]["addressCountry"]["name"]
                country_uri = line["address"]["addressCountry"]["sameAs"]
                wholestates_dicts[state_uri] = {}
                wholestates_dicts[state_uri]["name"] = state
                wholestates_dicts[state_uri]["country_uri"] = country_uri
                try:
                    stateDict = dicts[country_uri]["states"]
                    try:
                        stateDict[state_uri]["cities"][city_uri] = {}
                        stateDict[state_uri]["cities"][city_uri]["name"] = city
                        stateDict[state_uri]["cities"][city_uri]["snc"] = state + "," + country
                    except KeyError:
                        stateDict[state_uri] = {"cities": {city_uri: {"name": city, "snc": state + "," + country}},
                                                    "name": state}
                except KeyError:
                    dicts[country_uri] = {"states": {
                            state_uri: {"name": state, "cities": {city_uri: {"name": city, "snc": state + "," + country}}}},
                                              "name": country}
            except:
                state = ""
                country = ""


            if int(population) >= 25000:
                wholecities_dicts[city_uri] = {}
                wholecities_dicts[city_uri]["name"] = city
                wholecities_dicts[city_uri]["snc"] = state + "," + country
                wholecities_dicts[city_uri]['populationOfArea'] = population
            all_cities_dict[city_uri] = {}
            all_cities_dict[city_uri]['name'] = city
            all_cities_dict[city_uri]['snc'] = state + "," + country
            all_cities_dict[city_uri]['populationOfArea'] = population

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


def create_tagging_dict(json_file):
  states = set()
  countries = set()
  cities = set()

  for key in json_file["city"]:
    city = key.lower()
    cities.add(city)
    if len(city) >= 5:
      cities |= tagging.edits1(city)
  for key in json_file["state"]:
    state = key.lower()
    states.add(state)
    if len(state) >= 5:
      states |= tagging.edits1(state)
  for key in json_file["country"]:
    country = key.lower()
    countries.add(country)
    if len(country) >= 5:
      countries |= tagging.edits1(country)
  return {'city': {x:0 for x in cities},
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
    prior = create_prior_dict(input_path)
    prior_dict.write(prior)

    tagging_dict = codecs.open(output_path + "/tagging_dict.json", 'w')
    tagging_dict.write(json.dumps(create_tagging_dict(json.loads(prior))))