import json

# Given a path in json, return value if path, full path denoted by . (example address.name) exists, otherwise return ''
def get_value_json(path, doc, separator='.'):
    paths = path.strip().split(separator)
    for field in paths:
        if field in doc:
            doc = doc[field]
        else:
            return ''

    if type(doc) == dict:
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