import __init__
from schema_migration import SchemaImport
from loading import Load
import json
import sys,os
dir = os.path.dirname(__file__)

class Migration:
	
	def __init__(self,source=None,target=None):
		print 'migration started'

	def migrate(self):

		schema = SchemaImport()
		schema.create_tables()

		with open(os.path.join(dir, 'configuration.json'), 'r') as f: 
			data = json.load(f)

		if data['migrate']:			
			l = Load()
			l.load()

if __name__ == "__main__":
	m = Migration()
	m.migrate()