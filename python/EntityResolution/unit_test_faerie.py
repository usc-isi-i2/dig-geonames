import geoname_extractor
import json
import dictionaries

d = dictionaries.D("out/state_dict.json","out/all_city_dict.json","out/city_faerie.json","out/state_faerie.json","out/all_dict_faerie.json","out/prior_dict.json","out/tagging_dict.json")

queryline = {"uri":"123","locality":"Jiand Unar","region":"Sindh","country":"Pakistan"}
can = geoname_extractor.processDoc(queryline,d)
print json.dumps(can)