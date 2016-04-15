import faerie
import json

f = open("dicts/city_faerie.json")
j = json.load(f)

s = "New York City"
queryline = {"uri":"123","name":s}
can = faerie.processDoc(queryline,j)
print json.dumps(can)