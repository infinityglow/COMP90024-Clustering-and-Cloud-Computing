import couchdb
import time
import json
from shapely.geometry import shape, Point

with open('suburbData.json', 'r') as f:
    suburb_list = json.loads(f.read())

# start server
server = couchdb.Server("http://admin:admin@1172.26.130.24:5984/")
db = server['twitter']
size = len(db.view('_all_docs'))

start = time.time()

for i, row in enumerate(db.view('_all_docs')):
    id = row['id']
    doc = db[id]

    if 'coordinates' in doc.keys() and doc['coordinates']:
        for city in suburb_list.keys():
            for suburb in suburb_list[city]:
                polygon = shape(suburb_list[city][suburb])
                point = Point(doc['coordinates'])
                contained = polygon.contains(point)
                if contained:
                    doc['city'] = city.lower()
                    doc['suburb'] = suburb
                    break
            else:
                continue
            break
        else:
            doc['suburb'] = None
    db[id] = doc

    if (i + 1) % 1000 == 0:
        print("Processed: %d/%d" % (i+1, size))

print("Done")
end = time.time()
print("Time: %.2fs" % (end - start))

