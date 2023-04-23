from berkeleydb import db

# Create a database handle
db_handle = db.DB()

# Open a database connection
db_handle.open('metadata.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

# Define the table schema
table_schema = {
    'table_name': 'my_table',
    'columns': [
        {'column_name': 'id', 'type': 'int', 'nullable': False, 'length': None},
        {'column_name': 'name', 'type': 'str', 'nullable': False, 'length': 5, 'fk_ref_table': 'student', 'fk_ref_column': 'name'},
        {'column_name': 'age', 'type': 'int', 'nullable': True, 'length': 10},
    ],
    'primary_key': 'id',
    'foreign_key': 'name',
}

table_schema2 = {
    'table_name': 'my_second_table',
    'columns': [
        {'column_name': 'id', 'type': 'int', 'nullable': False},
        {'column_name': 'name', 'type': 'str', 'nullable': False, 'length': 15, 'fk_ref_table': 'my_table', 'fk_ref_column': 'name'},
        {'column_name': 'age', 'type': 'int', 'nullable': True},
    ],
    'primary_key': 'id',
    'foreign_key': 'name',
}

# Save the schema information to the database
table_name = table_schema['table_name']
table_name2 = table_schema2['table_name']
db_handle.put(table_name.encode(), str(table_schema).encode())
db_handle.put(table_name2.encode(), str(table_schema2).encode())

cursor = db_handle.cursor()
while x := cursor.next():
    print(x)

print(db_handle.get(b"hsallllllllllllllllqwerf"))

# Close the database connection
db_handle.close()
