# __author__ = 'zheng'

import json
import faerie
from pyspark.sql import Row
import Toolkit
import time

#get the city query line from input and return candidates in spark row format.
def processDoc(line, d):
    uri = line["uri"]
    city = line["locality"]
    state = line["region"]
    country = line["country"]
    start_time = time.clock()

    queryline = {"uri":uri,"name":country}
    country_can = faerie.processDoc(queryline, d.value.all_faerie_dict["countries_dict"])

    cities_can = search(country_can, uri, state, city, d)
    process_time = str((time.clock() - start_time)*1000)
    print "Time take to process: " + json.dumps(line) + " is " + process_time
    jsent = []
    if cities_can and 'entities' in cities_can:
        for eid in cities_can["entities"]:
            entity = cities_can["entities"][eid]
            snc = Toolkit.get_value_json(eid + "$snc", d.value.all_city_dict,'$')
            if snc != '':
                temp = Row(id=eid,value=entity["value"] + ","+snc,candwins=entity["candwins"])
                jsent.append(temp)
            else:
                temp = Row(id=eid,value=entity["value"] ,candwins=entity["candwins"])
                jsent.append(temp)
        jsdoc = Row(id=cities_can["document"]["id"],value=cities_can["document"]["value"] + ","+state+","+country)
        jsonline = Row(document=jsdoc,entities=jsent, processtime=process_time)
        return jsonline
    else:
        "cities_can has no entities:", city + "," + state + "," + country + "," + uri

    return ''

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
                    states_can["entities"] = dict(states_can["entities"],
                                          **temp["entities"])
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
    if cities_can:
        return cities_can
    else:
        return states_can

#search the city candidates.
def searchcity(states_can, uri, city, d):
    queryline = {"uri":uri,"name":city}
    cities_can = {}
    if states_can and states_can != {} and states_can["entities"] != {}:
        for state_uri in states_can["entities"]:
            country_uri = d.value.state_dict[state_uri]["country_uri"]
            if country_uri != '':
                if 'entities' in cities_can:
                    cities_can_add = faerie.processDoc(queryline, d.value.all_faerie_dict[country_uri][state_uri]["cities"],0.5)
                    if cities_can_add != {}:
                        cities_can["entities"] = dict(cities_can["entities"],
                                              **cities_can_add["entities"])
                else:
                    cities_can = faerie.processDoc(queryline, d.value.all_faerie_dict[country_uri][state_uri]["cities"],0.5)
            else:
                print "Line 73:" + state_uri
    else:
        cities_can = faerie.processDoc(queryline,d.value.city_faerie_dict,0.5)

    return cities_can

#run this whole extractor.
def run(d, input_rdd):

    candidates = input_rdd.map(lambda line : processDoc(json.loads(line), d))
    return candidates

