# __author__ = 'zheng'

import json
import faerie
import time

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
#get the city query line from input and return candidates in spark row format.
def processDoc(line, d):
    if line and 'uri' in line:
        uri = line["uri"]
        city = line["locality"]
        state = line["region"]
        country = line["country"]
        start_time = time.clock()
        queryline = {"uri":uri,"name":country}
        country_can = faerie.processDoc(queryline, d.value.all_faerie_dict["countries_dict"])

        cities_can = search(country_can, uri, state, city, d)
        process_time = str((time.clock() - start_time)*1000)
        if cities_can and 'entities' in cities_can:
            jsent = cities_can["entities"]
            # for eid in cities_can["entities"]:
            #     entity = cities_can["entities"][eid]
            #     snc = get_value_json(eid + "$snc", d.value.all_city_dict,'$')
            #     if snc != '':
            #         value_obj = dict(city=entity["value"],state=snc.split(",")[0],country=snc.split(",")[1])
            #         temp = dict(id=eid,value=value_obj,candwins=entity["candwins"])
            #         jsent.append(temp)
            #     else:
            #         value_obj = dict(state=entity["value"])
            #         temp = dict(id=eid,value=value_obj ,candwins=entity["candwins"])
            #         jsent.append(temp)
            jsdoc = dict(id=uri,value=city + ","+state+","+country)
            jsonline = dict(document=jsdoc,entities=jsent, processtime=process_time)
        else:
            jsdoc = dict(id=uri,value=city + ","+state+","+country)
            jsonline = dict(document=jsdoc,entities={}, processtime=process_time)

        return jsonline

#search the states based on the country candidates then search the city candidates based on the states candidates.
def search(country_can, uri, state, city, d):
    states_can = {}
    cities_can = {}
    if country_can and country_can != {} and country_can["entities"] != {}:
        for country_uri in country_can["entities"]:
            if state != "":
                queryline = {"uri":uri,"name":state}
                temp = faerie.processDoc(queryline, d.value.all_faerie_dict[country_uri]["states_dict"])
                if 'entities' in states_can:
                    if type(states_can["entities"]) == dict:
                        states_can["entities"] = dict(states_can["entities"],
                                            **temp["entities"])
                    elif temp["entities"] != {}:
                        states_can = temp
                    else:
                        states_can["entities"] += d.value.all_faerie_dict[country_uri]["states_dict"][4].values()
                else:
                    states_can = temp
            if states_can == None or states_can == {} or states_can["entities"] == {}:
                # if input state is empty, get the state uris from all_dicts in that country
                states_can["entities"] = d.value.all_faerie_dict[country_uri]["states_dict"][4].values()

            cities_can = searchcity(states_can,uri,city, d)
    else:
        if state != "":
            queryline = {"uri":uri,"name":state}
            states_can = faerie.processDoc(queryline, d.value.state_faerie_dict)

        cities_can = searchcity(states_can, uri, city, d)
    return cities_can


#search the city candidates.
def searchcity(states_can, uri, city, d):
    config = dict(dictionary={"id_attribute": "uri", "value_attribute": ["name"]},
                     document={"id_attribute": "uri", "value_attribute": ["name"]}, token_size=2, threshold=0.5)
    queryline = {"uri":uri,"name":city}
    cities_can = {}
    if states_can and states_can != {} and states_can["entities"] != {}:
        for state_uri in states_can["entities"]:
            country_uri = d.value.state_dict[state_uri]["country_uri"]
            if country_uri != '':
                if 'entities' in cities_can:
                    cities_can_add = faerie.processDoc(queryline, d.value.all_faerie_dict[country_uri][state_uri]["cities"],config)
                    if cities_can_add != {}:
                        cities_can["entities"] = dict(cities_can["entities"],
                                              **cities_can_add["entities"])
                else:
                    cities_can = faerie.processDoc(queryline, d.value.all_faerie_dict[country_uri][state_uri]["cities"],config)
            else:
                print "Line 73:" + state_uri
    else:
        cities_can = faerie.processDoc(queryline,d.value.city_faerie_dict,config)

    return cities_can

#run this whole extractor.
def run(d, input_rdd):

    candidates = input_rdd.map(lambda line : processDoc(json.loads(line), d))
    return candidates

