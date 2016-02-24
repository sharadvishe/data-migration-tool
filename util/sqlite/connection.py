import  os
import sqlite3

dir = os.path.dirname(__file__)
filename = os.path.join(dir, 'sqlitedb.db')
# print dir

class SQLiteFactory:
	def __init__(self):
		self.conn = sqlite3.connect(filename)

	def connect(self):
		return self.conn

	def disconnect(self):
		return self.conn.close()

		self.conn.execute("delete from co'lumns_meta'data")
