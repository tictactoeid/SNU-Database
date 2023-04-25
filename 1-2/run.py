import sys, os
from lark import Lark, Transformer, UnexpectedToken
from berkeleydb import db

DEBUG = False # TODO: make it False

with open("grammar.lark") as grammar:
    sql_parser = Lark(grammar.read(), start="command", lexer="basic")

if not os.path.exists('./DB'):
    os.makedirs('./DB')
metadata = db.DB()
metadata.open('./DB/metadata .db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
# table schema metadata file
# "metadata"라는 이름의 table과의 conflict를 막기 위해 파일명에 공백 삽입


class SQLTransformer(Transformer): # lark transformer class
    def create_table_query(self, items): # called when 'CREATE TABLE' query requested well
        # CREATE TABLE table_name table_element_list

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
                if column_name in fk_dict:
                    # {{fk_col_name : [ref_table_name, ref_table_name]}
                    ref_table = fk_dict.get(column_name)[0]
                    ref_column = fk_dict.get(column_name)[1]

                    ref_table_schema = metadata.get(ref_table.encode())
                    if ref_table_schema is None:
                        # ReferenceTableExistenceError
                        print("DB_2020-15127> Create table has failed: foreign key references non existing table")
                        return
                    flag_ = False

                    ref_table_schema = eval(ref_table_schema.decode())
                    for col_dict in ref_table_schema["columns"]:
                        if col_dict.get("column_name") == ref_column:
                            flag_ = True
                            if col_dict.get("type") != type or (type == "char" and col_dict.get("length") != length):
                                # ReferenceTypeError
                                print("DB_2020-15127> Create table has failed: foreign key references wrong type")
                                return
                            elif not ref_table_schema.get("primary_key"): # no primary key
                                # ReferenceNonPrimaryKeyError
                                print("DB_2020-15127> Create table has failed: foreign key references non primary key column")
                                return
                            elif ref_column not in ref_table_schema.get("primary_key"):
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
            col_list.append(col_dict["column_name"])
        for fk_name in fk_list:
            if fk_name not in col_list:
                # NonExistingColumnDefError
                print("DB_2020-15127> Create table has failed: \'" + fk_name + "\' does not exist in column definition")
                return

        for pk_name in primary_keys:
            if pk_name not in col_list:
                # NonExistingColumnDefError
                print("DB_2020-15127> Create table has failed: \'" + pk_name + "\' does not exist in column definition")
                return
        if fk_list: # foreign key exists
            for ref_pk in ref_table_schema.get("primary_key"):
                if ref_pk not in fk_list:
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
        if DEBUG:
            cursor = metadata.cursor()
            while x := cursor.next():
                print(x)

    def drop_table_query(self, items):
        table_name = items[2].children[0].lower()

        table_schema = metadata.get(table_name.encode())
        if table_schema is None:
            # NoSuchTable
            print("DB_2020-15127> No such table")
            return
        table_schema = eval(table_schema.decode())
        columns = table_schema["columns"]
        fk_list = table_schema["foreign_key"]

        cursor = metadata.cursor()
        while x := cursor.next():
            key, value = x
            current_table_schema = eval(value.decode())
            current_columns = current_table_schema["columns"]
            for col_dict in current_columns:
                try:
                    if col_dict["fk_ref_table"] == table_name:
                        # DropReferencedTableError
                        print("DB_2020-15127> Drop table has failed: \'" + table_name + "\' is referenced by other table")
                        return
                except KeyError:
                    pass
        metadata.delete(table_name.encode())
        try:
            os.remove('./DB/' + table_name + '.db')
        except FileNotFoundError:
            pass
        print("DB_2020-15127> \'" + table_name + "\' table is dropped")

        if DEBUG:
            cursor = metadata.cursor()
            while x := cursor.next():
                print(x)
    def explain_query(self, items):
        table_name = items[1].children[0].lower()
        table_schema = metadata.get(table_name.encode())
        if table_schema is None:
            # NoSuchTable
            print("DB_2020-15127> No such table")
            return
        table_schema = eval(table_schema.decode())
        columns = table_schema["columns"]
        pk_list = table_schema["primary_key"]
        fk_list = table_schema["foreign_key"]

        print("-----------------------------------------------------------------")
        print("table name [" + table_name + "]")
        format = '%-15s%-15s%-15s%-15s'
        str_out = format % ("column_name", "type", "null", "key")
        print(str_out)
        for col_dict in columns:
            column_name = col_dict["column_name"]
            type = col_dict["type"]
            nullable = col_dict["nullable"]

            if type == "char":
                length = col_dict["length"]
                type_str = type + '(' + str(length) + ')'
            else:
                type_str = type

            if nullable:
                nullable_str = 'Y'
            else:
                nullable_str = 'N'

            if column_name in pk_list and column_name in fk_list:
                key = "PRI/FOR"
            elif column_name in pk_list:
                key = "PRI"
            elif column_name in fk_list:
                key = "FOR"
            else:
                key = ""

            str_out = format % (column_name, type_str, nullable_str, key)
            print(str_out)
        print("-----------------------------------------------------------------")

    def describe_query(self, items):
        self.explain_query(items)

    def desc_query(self, items):
        self.explain_query(items)

    def insert_query(self, items):
        table_name = items[2].children[0].lower()

        table_schema = metadata.get(table_name.encode())
        if table_schema is None:
            # NoSuchTable
            print("DB_2020-15127> No such table")
            return
        table_schema = eval(table_schema.decode())
        columns = table_schema["columns"]
        pk_list = table_schema["primary_key"]
        fk_list = table_schema["foreign_key"]
        column_list = []
        for col_dict in columns:
            column_list.append(col_dict["column_name"])

        table_db = db.DB()
        table_db.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

        cursor = table_db.cursor()
        tuple_id = 0
        while x := cursor.next():
            tuple_id += 1

        column_order_query = []
        if not items[3]: # [(col_name1, col_name2, … )] has omitted
            column_order_query = column_list
        else:
            column_tree = items[3].children[1:-1]
            for x in column_tree:
                column_order_query.append(x.children[0].lower())

        values_tree = items[5].children[1:-1]
        values = []
        for x in values_tree:
            values.append(x.children[0].lower())
        values_dict = {}
        for col in column_list:
            values_dict[col] = values[column_order_query.index(col)]
        table_db.put(str(tuple_id).encode(), str(values_dict).encode()) # key is dummy

        if DEBUG:
            cursor = table_db.cursor()
            while x := cursor.next():
                print(x)

        table_db.close()
        print("DB_2020-15127> The row is inserted")
    def delete_query(self, items):
        print("DB_2020-15127> \'DELETE\' requested")
    def select_query(self, items):
        table_name = items[2].children[0].children[1].children[0].children[0].children[0].lower()
        table_schema = metadata.get(table_name.encode())
        if table_schema is None:
            # NoSuchTable
            print("DB_2020-15127> No such table")
            return
        table_schema = eval(table_schema.decode())
        table_db = db.DB()
        table_db.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
        column_list = []
        for col_dict in table_schema["columns"]:
            column_list.append(col_dict["column_name"])

        column_count = len(column_list)
        for i in range(column_count):
            print("+", end='')
            print('-' * 15, end='')
        print('+')
        strFormat = '| %-13s '
        for col in column_list:
            print(strFormat % col, end='')
        print('|')
        for i in range(column_count):
            print("+", end='')
            print('-' * 15, end='')
        print('+')

        cursor = table_db.cursor()
        while x := cursor.next():
            key, value = x
            tuple_dict = eval(value.decode())
            for col in column_list:
                print(strFormat % tuple_dict[col], end='')
            print('|')

        for i in range(column_count):
            print("+", end='')
            print('-' * 15, end='')
        print('+')

        table_db.close()

    def show_tables_query(self, items):
        print("------------------------")
        cursor = metadata.cursor()
        while x := cursor.next():
            key, value = x
            print(key.decode())
        print("------------------------")

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