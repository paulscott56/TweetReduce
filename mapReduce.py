
from pymongo import Connection
from pymongo.code import Code
import MySQLdb

# Open a connection to MySQL
mydb = MySQLdb.connect(user="root", passwd="", db="wordcloud")
c = mydb.cursor()

#'''
#Open a connection to MongoDb (localhost)
connection =  Connection()
db = connection.tweets

#Load map and reduce functions
m = Code(open('wordMap.js','r').read())
r = Code(open('wordReduce.js','r').read())

#Run the map-reduce query
results = db.tweets.map_reduce(m, r, "tweets")

ins = []
for result in results.find():
    c.execute("""INSERT INTO analysis (word, document, occurrances) VALUES (%s, %s, %s)""", (result['_id'], 'tweets', result['value']['count']))

#Remove any existing data
db.tweets.remove()    