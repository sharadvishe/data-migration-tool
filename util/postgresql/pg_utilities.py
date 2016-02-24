import psycopg2
import json
class Connection:
	def __init__(self,**args):

		self.conn = psycopg2.connect(database=args['database'], user=args['user'], password=args['password'],host=args['host'], port=args['port'])
		self.cur = self.conn.cursor()

	def disconnect(self):
		self.conn.close()

	def connect(self):
		self.cur = self.conn.cursor()
		return self.conn		

	def get_schemas(self,schema_name=None):

		if schema_name is None:
			schema_name = ''

		self.cur.execute("SELECT nspname FROM pg_catalog.pg_namespace where nspname=coalesce(nullif('"+schema_name+"',''),nspname)");

		rows = self.cur.fetchall()

		schemas = []

		for row in rows:

			schemas.append({"name":row[0]})

		return schemas

	def get_tables(self,schema_name = None):

		# print schema_name

		if schema_name is None:
			schema_name = ''

		# print "select table_name from information_schema.tables where table_schema=coalesce(nullif('"+schema_name+"',''),table_schema)"	

		self.cur.execute("select table_name from information_schema.tables where table_schema=coalesce(nullif('"+schema_name+"',''),table_schema)");

		rows = self.cur.fetchall()

		tables = []

		for row in rows:

			tables.append({"name":row[0]})

		return tables	

	def get_columns(self,table_name = None,schema_name = None):

		# print schema_name

		if table_name is None:
			table_name = ''

		if schema_name is None:
			schema_name = ''	

		# print "select table_name from information_schema.tables where table_schema=coalesce(nullif('"+schema_name+"',''),table_schema)"	

		self.cur.execute("select table_schema,table_name,column_name,ordinal_position,data_type,'null' as generic_datatype,numeric_precision,numeric_scale,character_maximum_length as size from information_schema.columns where table_schema=coalesce(nullif('"+schema_name+"',''),table_schema) and table_name=coalesce(nullif('"+table_name+"',''),table_name) order by table_schema asc,table_name asc,ordinal_position asc");

		rows = self.cur.fetchall()

		columns = []

		for row in rows:

			columns.append(
					{
					"table_schema":row[0],
					"table_name":row[1],
					"column_name":row[2],
					"ordinal_position":row[3],
					"datatype":row[4],
					"generic_datatype":row[5],
					"numeric_precision":row[6],
					"numeric_scale":row[7],
					"size":row[8]
					}
				)

		return columns
	
	def ddl_action(self,action_type=None,script=None):
		try:		
			self.cur.execute(script);
			self.conn.commit()
			return 1
		except: 
			return 0


if __name__ == "__main__":
	credentials = {"database":"survey_analytics","host":"localhost","port":"5432","user":"postgres","password":"postgres"}
	c = Connection(**credentials)
	schemas = c.get_schemas()
	tables = c.get_tables('public')
	columns = c.get_columns('dim_administrations')
	print json.dumps(tables,indent=4)

