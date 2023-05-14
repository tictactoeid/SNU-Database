import sys, os
from lark import Lark, Transformer, UnexpectedToken, Tree, Token
from berkeleydb import db

from ThreeValuedLogic import ThreeValuedLogic


DEBUG = False # TODO: make it False

with open("grammar.lark") as grammar:
    sql_parser = Lark(grammar.read(), start="command", lexer="basic")

if not os.path.exists('./DB'):
    os.makedirs('./DB')
metadata = db.DB()
metadata.open('./DB/metadata_.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
# table schema metadata file

def find_data(node: Tree, data_fields: list[str]) -> list[Tree]:
    """
    Find all nodes in the parse tree that have one of the specified data fields.
    """
    result = []
    if node.data in data_fields:
        result.append(node)
    for child in node.children:
        if isinstance(child, Tree):
            result.extend(find_data(child, data_fields))
    return result

def get_depth_str(node: Tree, target: str, depth: int = 0) -> int:
    if isinstance(node, Tree):
        if node.data == target:
            return depth
        for child in node.children:
            result = get_depth(child, target, depth+1)
            if result is not None:
                return result
    elif isinstance(node, str) and node == target:
        return depth
    return None

def get_depth(root, target, depth = 0):
    if isinstance(root, Tree):
        if root == target:
            return depth
        for child in root.children:
            result = get_depth(child, target, depth+1)
            if result is not None:
                return result
    elif isinstance(root, Token) and root == target: # str
        #print(root)
        #print(type(root))
        return depth
    return None

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
                    # {{fk_col_name : [ref_table_name, ref_column_name]}
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
        fk_ref_list = []
        for fk_dict in foreign_keys:
            fk_list += list(fk_dict.keys())
            fk_ref_list.append(list(fk_dict.values())[0])

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
            ref_table_list = set()
            for fk_dict in foreign_keys:
                # {{fk_col_name : [ref_table_name, ref_column_name]}
                ref_table = list(fk_dict.values())[0][0]
                #ref_column = list(fk_dict.values())[0][1]
                ref_table_list.add(ref_table)
                #ref_table_schema = metadata.get(ref_table.encode())
                #ref_table_schema = eval(ref_table_schema.decode())
                #for ref_pk in ref_table_schema.get("primary_key"):
            for ref_table in ref_table_list:
                ref_table_schema = metadata.get(ref_table.encode())
                ref_table_schema = eval(ref_table_schema.decode())
                ref_columns_curr_table = []
                for fk_dict in foreign_keys:
                    if ref_table == list(fk_dict.values())[0][0]:
                        ref_columns_curr_table.append(list(fk_dict.values())[0][1])

                for ref_pk in ref_table_schema.get("primary_key"):
                    if ref_pk not in ref_columns_curr_table:
                        # composite primary key의 일부만을 reference
                        # ReferenceNonPrimaryKeyError
                        print("DB_2020-15127> Create table has failed: foreign key references non primary key column")
                        return

            # TODO: fk column name과 referencing column name이 다른 경우 error 발생
            # TODO: 여러 table을 reference하는 경우

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
        metadata.delete(table_name.encode()) # delete schema
        try:
            os.remove('./DB/' + table_name + '.db') # delete data if exists
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
        table_schema = eval(table_schema.decode()) # get table schema
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

    # TODO: implement this
    # TODO: char length 초과하는 경우 잘라서
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

        # TODO
        #print(items[3])
        #print(type(items[3]))
        # INSERT INTO table_name [column_name_list] VALUES value_list
        #print(items[5])


        values_tree = items[5].children[1:-1]
        #print(values_tree)
        #print()
        values = []
        for x in values_tree:
            values.append(x.children[0].lower())
            print(x.children[0].type) # TODO: get value type

            #print(x.children[0])
        values_dict = {}
        for col in column_list:
            values_dict[col] = values[column_order_query.index(col)]

        print(values_dict)
        table_db.put(str(tuple_id).encode(), str(values_dict).encode()) # key is dummy, inserting value!

        if DEBUG:
            cursor = table_db.cursor()
            while x := cursor.next():
                print(x)

        table_db.close()
        print("DB_2020-15127> The row is inserted")

    # TODO: implement this
    def delete_query(self, items):
        #print(items[3].pretty())
        #print(items[3].children[1])
        #iter = items[3].find_data("comp_op")
        #for i in iter:
        #    print(i)
        #iter2 = items[3].find_data("comparison_predicate")
        #for i in iter2:
            #print(i) # comp_operand comp_op comp_operand
            #print(i.children[0].children[0]) # table name 생략 시 이게 None, error 발생
            #print(i.children[0].children[0].children[0].value)  # comp_opernad: table_name
            #print(i.children[0].children[0].data) # table_name
                            # table name 생략 시 error 발생.
            #print(i.children[0].children[1].children[0].value) # comp_operand: column_name
            #print(i.children[0].children[1].data) # column_name
            #print(i.children[1].children[0].value) # comp_op
            #print(i.children[2].children[0].children[0].value) # comp_operand
            #print(i.children[2].children[0].children[0])
            #print(i.children[2].children[0].data) # comparable_value

            #print(i.children[2].children[0].children[0].type)
        #iter3 = items[3].find_data("predicate")
       # for i in iter3:
        #    depth = get_depth(items[3], i)
       #     print(i)
        #    print(f"Depth of the node: {depth}")

        #print(get_depth(items[3], "and"))
        #print(get_depth(items[3], "or"))
        #print(get_depth(items[3], "not"))

        #iteristhispossible = find_data(items[3], ["and_op", "or_op", "not_op", "predicate"])
            #lambda x: x == "and_op" or x=="or_op" or x=="not_op")
        #for j in iteristhispossible:
        #    print(j)

        """iternull = items[3].find_data("null_predicate")
        for i in iternull:
            print()
            print(i)
            print(i.children[0]) # i.children[0].children[0].value table_name
            print(i.children[1].children[0].value) # column_name
            print(i.children[2].children[0].value) # is
            print(i.children[2].children[1]) # [not] or None
            print(i.children[2].children[2].value) # null
            print() """
        where_clause = []
        where_iter = find_data(items[3], ["and_op", "or_op", "not_op", "comparison_predicate", "null_predicate"])
        for i in where_iter:
            if i.data == "comparison_predicate":  # comp_operand comp_op comp_operand
                print(i.pretty())
                print(i.children[2].children[0])
                print(i.children[0].children[0].children[0])  # value
                print(i.children[0].children[0].children[0].type)  # comp_val_type
                #if i.children[0].children[0] and i.children[0].children[1].data == "column_name":
                print(i.children[1].children[0]) # operator
                print(i.children[0].children[0].children[0]) # table_name
                print(i.children[0].children[1].children[0]) # col_name
                #elif i.children[0].children[1].data != "column_name":
                print()





    # TODO: implement this
    def select_query(self, items):
        #table_name = items[2].children[0].children[1].children[0].children[0].children[0].lower()
        if items[1].children == []: # select *
            pass

        print(items[2].pretty())
        #print(items[2].pretty())
        #for i in items[2].find_data("referred_table"):
            #print(i.children[0].children[0].value) # table_name
       # for i in items[1].find_data("selected_column"):
        #    if i.children[0] is not None:
        #        print(i.children[0].children[0].value) # table_name
       #     print(i.children[1].children[0].value) # column_name
       #     print()
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


#output = sql_parser.parse('create table account (account_number int not null, branch_name char(15), primary key(account_number, branch_name), foreign key(branch_name) references table_name(ID), foreign key(account_number) references mytable(cols));')
#output = sql_parser.parse("drop table name;")
#output = sql_parser.parse("explain account;")
#output = sql_parser.parse("insert into table_name values(123, \"hello\", \"umm\");")
#output = sql_parser.parse("insert into table_name (id, name) values(123, \"hello\", \"umm\");")
#output = sql_parser.parse("insert into ogamdo (name, a_hae, children, id) values ('choi', 13, 2020-01-01, 5678);")
#output = sql_parser.parse("insert into ref (id, test) values (10, 'hello');")

#output = sql_parser.parse("delete from account where table_name.branch_name = 'Perryridge' and (table_name.test > 100 or OR_CLAUSE is null) and not (100 = parenthesized_not_clause);")

#output = sql_parser.parse("delete from account where or_clause = 100 or and_clause_first > 10 and and_clause_second = 30;")

output = sql_parser.parse("select customer_name, borrower.loan_number, amount from borrower, loan, table_name where table_name.test > 100 or OR_CLAUSE is null;")

#output = sql_parser.parse("select * from customer;")

transformer.transform(output)
#strFormat = '%-10s%-10s%-10s\n'
#strOut = strFormat % ('abc','def','cgh')
#print(len("column_name		"))
#print(strOut)
