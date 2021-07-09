import couchdb

server = couchdb.Server("http://admin:admin@172.26.130.24:5984/")

cities = ['adelaide', 'brisbane', 'melbourne', 'perth', 'sydney']

# for city in cities:
server.delete('twitter')

