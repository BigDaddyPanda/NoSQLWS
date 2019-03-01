import os
import datetime,pprint
# from pymongo import MongoClient
# First, declare a Document/Collection pair (a "model"):
# from mongokat import Collection, Document
from pymongo import MongoClient

client = MongoClient()

db = client["bibliotest"]
db["books"].delete_many({})
db["tenancy"].delete_many({})
Tenancy = db["tenancy"]
Books = db["books"]