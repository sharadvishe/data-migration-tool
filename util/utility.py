import __init__
from util.sqlite.connection import SQLiteFactory
# from util.postgresql.pg_utilities import Connection

class Utility:
	def __init__(self):

		self.conn = SQLiteFactory()
		self.cur = self.conn.connect()
		self.source_conn = None
		self.target_conn = None

	def get_source(self,vendor=None):
		if vendor is None:
			vendor = ''
		source = {}
		row = self.cur.execute("select id,name from vendors where name=coalesce(nullif('"+vendor+"',''),name)")	
		for data in row:
			source = {"id":data[0],"name":data[1]}

		return source
		
	def get_target(self,target=None):
		if target is None:
			target = ''
		target = {}	
		row = self.cur.execute("select id,name from vendors where name=coalesce(nullif('"+target+"',''),name)")	
		for data in row:
			target = {"id":data[0],"name":data[1]}

		return target			

	def get_source_connection(self,vendor_id=None,credentials=None):
		# cur = self.conn.connect()
		modules = str(self.cur.execute("select modules from vendor_modules where vendor_id="+str(vendor_id)).fetchone()[0])
		exec modules
		self.source_conn = Connection(**credentials)
		return self.source_conn

	def get_target_connection(self,vendor_id=None,credentials=None):
		cur = self.conn.connect()
		modules = str(self.cur.execute("select modules from vendor_modules where vendor_id="+str(vendor_id)).fetchone()[0])
		exec modules
		self.target_conn = Connection(**credentials)
		return self.target_conn		

	def get_table_ddl(self,**args):
			
		column_list = []
		column_details = []

		# cur = self.conn.connect()
		column = ""
		rows = self.cur.execute("select column_name,target_datatype,generic_datatype,size,numeric_precision,numeric_scale,ordinal_position from columns_metadata where target_vendor_id="+ str(args['target_vendor_id']) +" and table_name='" + args['table_name'] + "'order by ordinal_position asc")
		# print ("select column_name,target_datatype,generic_datatype,size,numeric_precision,numeric_scale,ordinal_position from columns_metadata where target_vendor_id="+ str(args['target_vendor_id']) +" and table_name='" + args['table_name'] + "'order by ordinal_position asc")
		for row in rows:
			column_list.append({"column_name":row[0],"datatype":row[1],"generic_datatype":row[2],"size":row[3],"numeric_precision":row[4],"numeric_scale":row[5]})			
		
		syntax = self.cur.execute("select syntax from sql_actions where action_name='CREATE TABLE' and vendor_id="+str(args['target_vendor_id']))

		# print syntax['syntax'][0]
		for data in syntax:
			syntax =  data[0]

		for i in range(0,len(column_list)):	

			if  column_list[i]['generic_datatype'] == 'String':				
				column = "".join([column_list[i]['column_name'],' ',column_list[i]['datatype'],'(',str(column_list[i]['size']),')'])
			elif (column_list[i]['generic_datatype'] == 'Numeric') or (column_list[i]['generic_datatype'] == 'Decimal'):				
				column = "".join([column_list[i]['column_name'],' ',column_list[i]['datatype'],'(',str(column_list[i]['numeric_precision']),',',str(column_list[i]['numeric_scale']),')'])
			else:
				column = "".join([column_list[i]['column_name'],' ',column_list[i]['datatype']])
				


			column_details.append(column)
			
		column_details = ",".join(column_details)

		# print column_details

		REPLACEMENTS = [
		    ("$TABLE_NAME", args['table_name']),
		    ("$COLUMN_NAME $DATATYPE_NAME", column_details)
		    ]
		for entity, replacement in REPLACEMENTS:
		    syntax = syntax.replace(entity, replacement)

		# self.conn.disconnect()		    

		return syntax



	def load_column_metadata(self,source=None,source_vendor_id=None,target_vendor_id=None):

		self.get_source_connection(vendor_id=source_vendor_id,credentials=source)
		# cur = self.conn.connect()	

		schemas = self.get_schemas('public')
		tables = self.get_tables('public')
		columns = self.get_columns(schema_name='public')
		self.cur.execute("delete from columns_metadata")
		self.cur.commit()

		for data in columns:
			if data['numeric_precision'] is None:
				data['numeric_precision'] = '0'
			if data['numeric_scale'] is None:
				data['numeric_scale'] = '0'	
			if data['size'] is None:
				data['size'] = 0

			if source_vendor_id == target_vendor_id:
				self.cur.execute("INSERT INTO columns_metadata (schema_name,table_name,column_name,ordinal_position,vendor_datatype,target_datatype,generic_datatype,numeric_precision,numeric_scale,size,source_vendor_id,target_vendor_id) VALUES ('" + data['table_schema'] + "','"+ data['table_name'] + "','" + data['column_name'] + "','" + str(data['ordinal_position']) + "','" + data['datatype'] + "','" + data['datatype'] + "','" + data['generic_datatype'] +"','" + str(data['numeric_precision']) + "','" + str(data['numeric_scale']) + "','" + str(data['size']) +  "'," + source_vendor_id + "," + target_vendor_id +")")
			else:				
				self.cur.execute("INSERT INTO columns_metadata (schema_name,table_name,column_name,ordinal_position,vendor_datatype,generic_datatype,numeric_precision,numeric_scale,size,source_vendor_id,target_vendor_id) VALUES ('" + data['table_schema'] + "','"+ data['table_name'] + "','" + data['column_name'] + "','" + str(data['ordinal_position']) + "','" + data['datatype'] + "','" + data['generic_datatype'] +"','" + str(data['numeric_precision']) + "','" + str(data['numeric_scale']) + "','" + str(data['size']) +  "'," + source_vendor_id + "," + target_vendor_id +")")				
				self.cur.execute("update columns_metadata set generic_datatype_id =(select id from generic_datatypes g where id in(select generic_datatype_id from vendor_datatype_mapping where vendor_id="+source_vendor_id+" and datatype = columns_metadata.vendor_datatype))")
				self.cur.execute("update columns_metadata set generic_datatype =(select name from generic_datatypes g where id = columns_metadata.generic_datatype_id)")
				self.cur.execute("update columns_metadata set target_datatype =Coalesce((select datatype from vendor_datatype_mapping v where v.vendor_id ="+target_vendor_id+"  and v.generic_datatype_id = columns_metadata.generic_datatype_id ),'Text')")

			self.cur.commit()

		# self.conn.disconnect()

	def get_schemas(self,schema_name=None):
		schemas = self.source_conn.get_schemas(schema_name=schema_name)
		return schemas

	def get_tables(self,schema_name=None):
		tables = self.source_conn.get_tables(schema_name=schema_name)
		return tables

	def get_columns(self,schema_name=None,table_name=None):
		columns = self.source_conn.get_columns(schema_name=schema_name,table_name=table_name)
		return columns

	def ddl_action(self,action_type=None,script=None):
		status = self.target_conn.ddl_action(action_type=action_type,script=script)
		return status	


if __name__ == "__main__":
	u = Utility()
	crd = {"database":"source","host":"localhost","port":"5432","user":"postgres","password":"postgres"}
	# u.load_column_metadata(source=crd,source_vendor_id=1,target_vendor_id=1)
	u.get_source_connection(vendor_id=1,credentials=crd)
	# print u.get_columns(schema_name='public')

	# print u.get_table_ddl(table_name = "emp",target_vendor_id = 1)
