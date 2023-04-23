import sys
from lark import Lark, Transformer, UnexpectedToken
from berkeleydb import db

with open("grammar.lark") as grammar:
    sql_parser = Lark(grammar.read(), start="command", lexer="basic")

metadata = db.DB()
metadata.open('./DB/metadata.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
# table schema metadata file

class SQLTransformer(Transformer): # lark transformer class
    def create_table_query(self, items): # called when 'CREATE TABLE' query requested well
        # CREATE TABLE table_name table_element_list
        # TODO: 예외 처리

        table_name = items[2].children[0].lower()
        column_definition_iter = items[3].find_data("column_definition")
        table_constraint_definition_iter = items[3].find_data("table_constraint_definition")

        if metadata.get(table_name.encode()) is not None:
            # TableExistenceError
            print("DB_2020-15127> Create table has failed: table with the same name already exists")
            return

        columns = []
        primary_keys = []
        foreign_keys = []
        flag = 0
        for j in table_constraint_definition_iter: # check table constraint
            if (j.children[0].children[0].lower() == "primary"): # primary key
                if flag == 1:
                    print("DB_2020-15127> Create table has failed: primary key definition is duplicated")
                    return # DuplicatePrimaryKeyDefError
                flag = 1
                # PRIMARY KEY column_name_list
                for k in j.find_data("column_name"):
                    primary_keys.append(k.children[0].lower())
            else: # foreign key
                # FOREIGN KEY column_name_list REFERENCES table_name column_name_list
                fk_col_name = j.children[0].children[2].children[1].children[0].lower()
                ref_table_name = j.children[0].children[4].children[0].lower()
                ref_col_name = j.children[0].children[5].children[1].children[0].lower()

                fk_dict = {fk_col_name : [ref_table_name, ref_col_name]}
                foreign_keys.append(fk_dict)

        for i in column_definition_iter: # check column definition
            # column_name data_type [NOT NULL]
            column_name = i.children[0].children[0].lower()
            for col_dict in columns:
                if col_dict.get("column_name") == column_name:
                    print("DB_2020-15127> Create table has failed: column definition is duplicated")
                    return
                    # DuplicateColumnDefError

            type = i.children[1].children[0].lower()

            if column_name in primary_keys: # check if nullable or not
                nullable = False
            elif (i.children[2]): # children[2], children[3]: not, null / None
                nullable = False
            else:
                nullable = True

            column_dict = {"column_name": column_name, "type": type, "nullable": nullable}
            length = 0
            if type == "char":
                length = int(i.children[1].children[2])
                if (length < 1):
                    print("DB_2020-15127> Char length should be over 0")
                    return
                column_dict["length"] = length

            for fk_dict in foreign_keys:
                if column_name in fk_dict: # TODO: error handling about foreign key
                    # {{fk_col_name : [ref_table_name, ref_table_name]}
                    ref_table = fk_dict.get(column_name)[0]
                    ref_column = fk_dict.get(column_name)[1]

                    ref_table_schema = metadata.get(fk_ref_table.encode())
                    if ref_table_schema is None:
                        # ReferenceTableExistenceError
                        print("DB_2020-15127> Create table has failed: foreign key references non existing table")
                        return
                    flag_ = False
                    for col_dict in ref_table_schema["columns"]:
                        if col_dict.get("column_name") == ref_column:
                            flag_ = True
                            if col_dict.get("type") != type or (type == "char" and col_dict.get("length") != length):
                                # ReferenceTypeError
                                print("DB_2020-15127> Create table has failed: foreign key references wrong type")
                                return
                            if ref_column not in col_dict.get("primary_key"):
                                # ReferenceNonPrimaryKeyError
                                print("DB_2020-15127> Create table has failed: foreign key references non primary key column")
                                return
                        if not flag_:
                            # ReferenceColumnExistenceError
                            print("DB_2020-15127> Create table has failed: foreign key references non existing column")
                            return

                    column_dict["fk_ref_table"] = ref_table
                    column_dict["fk_ref_column"] = ref_column
            columns.append(column_dict)

        fk_list = []
        for fk_dict in foreign_keys:
            fk_list += list(fk_dict.keys())

        col_list = []
        for col_dict in columns:
            col_list += list(col_dict.keys())

        for fk_name in fk_list:
            if fk_name not in col_list:
                # NonExistingColumnDefError
                print("DB_2020-15127> Create table has failed: \'[" + fk_name + "]\' does not exist in column definition")
                return
        for pk_name in primary_keys:
            if pk_name not in col_list:
                # NonExistingColumnDefError
                print("DB_2020-15127> Create table has failed: \'[" + pk_name + "]\' does not exist in column definition")
                return
            if pk_name not in fk_list:
                # composite primary key의 일부만을 reference
                # ReferenceNonPrimaryKeyError
                print("DB_2020-15127> Create table has failed: foreign key references non primary key column")
                return

        table_schema = {
            "table_name" : table_name,
            "columns" : columns,
            "primary_key" : primary_keys,
            "foreign_key" : fk_list
        }
        metadata.put(table_name.encode(), str(table_schema).encode())
        print("DB_2020-15127> \'" + table_name + "\' table is created") # CreateTableSuccess

        cursor = metadata.cursor() # TODO: delete this (for debug)
        while x := cursor.next():
            print(x)

    def drop_table_query(self, items):
        print("DB_2020-15127> \'DROP TABLE\' requested")

    def explain_query(self, items):
        print("DB_2020-15127> \'EXPLAIN\' requested")
    def describe_query(self, items):
        print("DB_2020-15127> \'DESCRIBE\' requested")

    def desc_query(self, items):
        print("DB_2020-15127> \'DESC\' requested")

    def insert_query(self, items):
        print("DB_2020-15127> \'INSERT\' requested")

    def delete_query(self, items):
        print("DB_2020-15127> \'DELETE\' requested")
    def select_query(self, items):
        print("DB_2020-15127> \'SELECT\' requested")

    def show_tables_query(self, items):
        print("DB_2020-15127> \'SHOW TABLES\' requested")
    def update_query(self, items):
        print("DB_2020-15127> \'UPDATE\' requested")

transformer = SQLTransformer()

while True: # prompt
    data = input("DB_2020-15127> ")
    while (data.rstrip() == '') or (data.rstrip()[-1] != ';'):
        data += ' '
        data += input()
    querys = data.rstrip().split(';')
    for query in querys[0:-1]:
        if query.strip() == "exit":
            sys.exit()
        try:
            output = sql_parser.parse(query + ';')
            transformer.transform(output)
        except UnexpectedToken as E:
            print("DB_2020-15127> Syntax error")
            break

metadata.close()