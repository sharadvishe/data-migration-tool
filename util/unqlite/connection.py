import  os
import sqlite3
from unqlite import UnQLite

dir = os.path.dirname(__file__)
filename = os.path.join(dir, 'uniqlite.db')
# print dir

class UniqLiteFactory:
	def __init__(self):
		self.conn = None

	def connect(self):
		self.conn = UnQLite(filename)
		return self.conn 

	def disconnect(self):
		self.conn.close()

	def create_collection(self,name):
		db = self.connect()
		collection = db.collection(name)
		collection.create()
		print "collection "+name+" created successfully "
		self.disconnect()

	def drop_collection(self,name):
		db = self.connect()
		collection = db.collection(name)
		collection.drop()
		print "collection "+name+" droped successfully "
		self.disconnect()		

	def insert(self,collection_name,data=None):
		db = self.connect()
		collection_name = db.collection(collection_name)
		collection_name.store(data)
		self.disconnect()


