import __init__
import sys,os
from util.sqlite import *
from util.utility import *
from loading import Load
import json

dir = os.path.dirname(__file__)

class SchemaImport:
	def __init__(self):
		print "Schema Migration started"

	
	def create_tables(self):

		with open(os.path.join(dir, 'configuration.json'), 'r') as f: 
			data = json.load(f)

		s_credentials = {"database":data['source']['database'],"host":data['source']['host'],"port":data['source']['port'],"user":data['source']['username'],"password":data['source']['password']}
		t_credentials = {"database":data['target']['database'],"host":data['target']['host'],"port":data['target']['port'],"user":data['target']['username'],"password":data['target']['password']}

		u = Utility()

		source_id = u.get_source(vendor=data['source']['vendor'])["id"]
		target_id = u.get_source(vendor=data['target']['vendor'])["id"]

		u.get_source_connection(vendor_id=source_id,credentials=s_credentials)
		u.get_target_connection(vendor_id=target_id,credentials=t_credentials)
		u.load_column_metadata(source=s_credentials,source_vendor_id=str(source_id),target_vendor_id=str(target_id))
		
		tables = u.get_tables('public')

		for table in tables:
			script = u.get_table_ddl(table_name = table['name'],target_vendor_id = target_id)
			status = u.ddl_action(script=script)

			if status == 1:
				print "Table "+table['name']+" Created successfully."
			else:
				print "Table "+table['name']+" already exist."