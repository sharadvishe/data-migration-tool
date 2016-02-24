import __init__
import sys,os
from util.sqlite import *
from util.utility import *
import json
import petl as etl
import psycopg2

dir = os.path.dirname(__file__)

class Load:
	
	def __init__(self,source=None,target=None):
		print 'Loading started'

	def load(self):

		with open(os.path.join(dir, 'configuration.json'), 'r') as f: 
			data = json.load(f)

		s_credentials = {"database":data['source']['database'],"host":data['source']['host'],"port":data['source']['port'],"user":data['source']['username'],"password":data['source']['password']}
		t_credentials = {"database":data['target']['database'],"host":data['target']['host'],"port":data['target']['port'],"user":data['target']['username'],"password":data['target']['password']}

		u = Utility()

		source_id = u.get_source(vendor=data['source']['vendor'])["id"]
		target_id = u.get_source(vendor=data['target']['vendor'])["id"]

		source = u.get_source_connection(vendor_id=source_id,credentials=s_credentials).connect()
		target = u.get_target_connection(vendor_id=target_id,credentials=t_credentials).connect()

		tables = u.get_tables('public')
		# table = etl.fromdb(source,'select * from vendor_modules')
		# etl.todb(table, target, 'vendor_modules')
		for doc in tables:

			query = "select * from "+doc['name']
			table = etl.fromdb(source,query)
			if len(table) > 1:
				etl.todb(table, target, doc['name'])
			# print len(table)


			print "Loading of table "+doc['name']+" completed"

if __name__ == "__main__":
	m = Load()
	m.load()