import sys, os
from lark import Lark, Transformer, UnexpectedToken, UnexpectedCharacters, UnexpectedInput, UnexpectedEOF, Tree, Token
from berkeleydb import db

from BooleanFactor import *
from ThreeValuedLogic import *

DEBUG = True # TODO: make it False

with open("grammar.lark") as grammar:
    sql_parser = Lark(grammar.read(), start="command", lexer="basic")

if not os.path.exists('./DB'):
    os.makedirs('./DB')
metadata = db.DB()
metadata.open('./DB/metadata_.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
# table schema metadata file

def find_data_multiple(root, fields):
    # tree.find_data()릏 여러 data를 찾을 수 있도록 확장
    result = []
    if root.data in fields:
        result.append(root)
    for child in root.children:
        if isinstance(child, Tree):
            result.extend(find_data_multiple(child, fields))
    return result
def get_depth(root, target, depth = 0):
    # tree에서 특정 node의 depth 찾기
    if isinstance(root, Tree):
        if root == target:
            return depth
        for child in root.children:
            result = get_depth(child, target, depth+1)
            if result is not None:
                return result
    elif isinstance(root, Token) and root == target:
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

        values_tree = items[5].children[1:-1]
        values = []
        values_type = []
        for x in values_tree:
            values.append(x.children[0].lower())
            values_type.append(x.children[0].type.lower())

        # TODO : check if valid

        if len(column_order_query) != len(values):
            # InsertTypeMismatchError
            # column과 value의 수가 불일치
            # column을 명시하지 않았을 때도 포함
            print("DB_2020-15127> Insertion has failed: Types are not matched")
            return

        # TODO: InsertColumnExistenceError(#colName) 여기서 먼저 처리해야 함

        for col_query in column_order_query:
            if col_query not in column_list:
                # InsertColumnExistenceError(#colName)
                print("DB_2020-15127> Insertion has failed: \'" + col_query + "\' does not exist")
                return

        values_dict = {}
        values_type_dict = {}
        for col in column_list:
            values_dict[col] = values[column_order_query.index(col)]
            values_type_dict[col] = values_type[column_order_query.index(col)]
        # {"id" : 10, "name" : "jimin"}


        for col in values_dict:
            value_current = values_dict[col]
            value_type_current = values_type_dict[col]
            for col_dict in columns:
                if col_dict["column_name"] == col:
                    type_current = col_dict["type"]
                    nullable_current = col_dict["nullable"]
                    if type_current == "char":
                        length_current = col_dict["length"]
                    else:
                        length_current = None
                    break
            if (value_type_current == "str" and type_current != "char") or (value_type_current == "int" and type_current != "int") or (value_type_current == "date" and type_current != "date"):
                # InsertTypeMismatchError
                # column과 value의 type이 불일치
                print("DB_2020-15127> Insertion has failed: Types are not matched")
                return
            if not nullable_current and str(value_current).lower() == "null":
                # InsertColumnNonNullableError(#colName)
                print("DB_2020-15127> Insertion has failed: \'" + col + "\' is not nullable")
                return
            if type_current == "char" and len(value_current) > length_current:
                print(length_current)
                values_dict[col] = "\'" + value_current[1:length_current+1] + "\'" # truncate



        table_db.put(str(tuple_id).encode(), str(values_dict).encode()) # key is dummy, inserting value!

        if DEBUG:
            cursor = table_db.cursor()
            while x := cursor.next():
                print(x)

        table_db.close()
        print("DB_2020-15127> The row is inserted")

    # TODO: implement this
    def delete_query(self, items):
        # DELETE FROM table_name [where_clause]
        # WHERE boolean_expr

        table_name = items[2].children[0].lower()
        table_schema = metadata.get(table_name.encode())
        if table_schema is None:
            # NoSuchTable
            print("DB_2020-15127> No such table")
            return
        table_schema = eval(table_schema.decode())

        columns = table_schema["columns"]
        column_list = []
        for col_dict in columns:
            column_list.append(col_dict["column_name"])

        table_db = db.DB()
        table_db.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

        if not items[3]: # [where_clause] has omitted
            # delete all tuples
            cursor = table_db.cursor()
            cnt = 0
            while x := cursor.next():
                key, value = x
                table_db.delete(key)
                cnt += 1
            print("DB_2020-15127> \'" + cnt + "\' row(s) are deleted")
            return

        where_clause = []
        where_iter = find_data_multiple(items[3], ["and_op", "or_op", "not_op", "comparison_predicate", "null_predicate"])
        for i in where_iter:
            if i.data == "comparison_predicate":  # comp_operand comp_op comp_operand
                operator = i.children[1].children[0].value
                if i.children[0].children[0]:
                    if i.children[0].children[0].data == "table_name":
                        operand_1 = ComparisonOperand(type="column_name", table_name=i.children[0].children[0].value, column_name=i.children[0].children[1].value)
                    elif i.children[0].children[0].data == "comparable_value":
                        operand_1 = ComparisonOperand(type="comparable_value", value=i.children[0].children[0].children[0].data, comparable_value_type=i.children[0].children[0].children[0].type)
                else:
                    # i.children[0].children[0] is None
                    # table_name has omitted
                    operand_1 = ComparisonOperand(type="column_name", table_name=None, column_name=i.children[0].children[1].value)

                if i.children[2].children[0]:
                    if i.children[2].children[0].data == "table_name":
                        operand_2 = ComparisonOperand(type="column_name", table_name=i.children[2].children[0].value, column_name=i.children[2].children[1].value)
                    elif i.children[2].children[0].data == "comparable_value":
                        operand_2 = ComparisonOperand(type="comparable_value", value=i.children[2].children[0].children[0].data, comparable_value_type=i.children[2].children[0].children[0].type)
                else:
                    operand_2 = ComparisonOperand(type="column_name", table_name=None, column_name=i.children[2].children[1].value)


                if operand_1.type == "column_name":
                    if operand_1.table_name and operand_1.table_name != table_name:
                        # WhereTableNotSpecified
                        print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                        return
                    if operand_1.column_name not in column_list:
                        # WhereColumnNotExist
                        print("DB_2020-15127> Where clause trying to reference non existing column")
                        return
                    for col_dict in columns:
                        if col_dict["column_name"] == operand_1.column_name:
                            operand_1.comparable_value_type = col_dict["type"]
                            break

                if operand_2.type == "column_name":
                    if operand_2.table_name and operand_2.table_name != table_name:
                        # WhereTableNotSpecified
                        print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                        return
                    if operand_2.column_name not in column_list:
                        # WhereColumnNotExist
                        print("DB_2020-15127> Where clause trying to reference non existing column")
                        return
                    for col_dict in columns:
                        if col_dict["column_name"] == operand_2.column_name:
                            operand_2.comparable_value_type = col_dict["type"]
                            break

                if operand_1.comparable_value_type == "null" or operand_2.comparable_value_type == "null":
                    pass
                elif operand_1.comparable_value_type != operand_2.comparable_value_type:
                    # WhereIncomparableError
                    print("DB_2020-15127> Where clause trying to compare incomparable values")
                    return
                where_clause.append(ComparisonPredicate(operand_1=operand_1, operator=operator, operand_2=operand_2))

            elif i.data == "null_predicate":
                # [table_name "."] column_name null_operation
                # column_name, table_name = None, is_not_null = False
                column_name_current = i.children[1].children[0].value
                if i.children[0]:
                    table_name_current = i.children[0].children[0].value
                else: # [table_name] has omitted
                    table_name_current = None
                if i.children[2].children[1]: # is NOT null
                    is_not_null = True
                else: # is null
                    is_not_null = False
                where_clause.append(NullPredicate(column_name=column_name_current, table_name=table_name_current, is_not_null=is_not_null))

            elif i.data == "and_op":
                where_clause.append(BooleanOperator(type="and", depth=get_depth(items[3], i)))
            elif i.data == "or_op":
                where_clause.append(BooleanOperator(type="or", depth=get_depth(items[3], i)))
            elif i.data == "not_op":
                where_clause.append(BooleanOperator(type="not", depth=get_depth(items[3], i)))


        """
        # dummy code
        comparison_predicate_iter = items[3].find_data("comparison_predicate")
        comparisons = []
        for i in comparison_predicate_iter:
            # comp_operand comp_op comp_operand
            operator = i.children[1].children[0].value
            if i.children[0].children[0]:
                if i.children[0].children[0].data == "table_name":
                    operand_1 = {"type" : "column_name", "table_name" : i.children[0].children[0].value.lower(), "column_name" : i.children[0].children[1].value.lower()}
                elif i.children[0].children[0].data == "comparable_value":
                    operand_1 = {"type" : "comparable_value", "value" : i.children[0].children[0].children[0].data, "compare_type" : i.children[0].children[0].children[0].type.lower()}
            else:
                # i.children[0].children[0] is None
                # table_name omitted
                operand_1 = {"type" : "column_name", "table_name" : None, "column_name" : i.children[0].children[1].value.lower()}

            if i.children[2].children[0]:
                if i.children[2].children[0].data == "table_name":
                    operand_2 = {"type": "column_name", "table_name": i.children[2].children[0].value.lower(),
                                 "column_name": i.children[0].children[1].value.lower()}
                elif i.children[2].children[0].data == "comparable_value":
                    operand_2 = {"type" : "comparable_value", "value" : i.children[2].children[0].children[0].data, "compare_type" : i.children[2].children[0].children[0].type.lower()}
            else:
                operand_2 = {"type": "column_name", "table_name": None, "column_name": i.children[2].children[1].value.lower()}

            if operand_1["type"]  == "column_name":
                if operand_1["table_name"] and operand_1["table_name"] != table_name:
                    # WhereTableNotSpecified
                    print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                    return
                if operand_1["column_name"] not in column_list:
                    # WhereColumnNotExist
                    print("DB_2020-15127> Where clause trying to reference non existing column")
                    return
                for col_dict in columns:
                    if col_dict["column_name"] == operand_1["column_name"]:
                        operand_1["compare_type"] = col_dict["type"]
                        break

            if operand_2["type"]  == "column_name":
                if operand_2["table_name"] and operand_1["table_name"] != table_name:
                    # WhereTableNotSpecified
                    print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                    return
                if operand_2["column_name"] not in column_list:
                    # WhereColumnNotExist
                    print("DB_2020-15127> Where clause trying to reference non existing column")
                    return
                for col_dict in columns:
                    if col_dict["column_name"] == operand_2["column_name"]:
                        operand_2["compare_type"] = col_dict["type"]
                        break

            if operand_1["compare_type"] == "null" or operand_2["compare_type"] == "null":
                pass
            elif operand_1["compare_type"] != operand_2["compare_type"]:
                # WhereIncomparableError
                print("DB_2020-15127> Where clause trying to compare incomparable values")
                return
            comparisons.append([operand_1, operator, operand_2])
            # dummy code end
            """

    # TODO: implement this
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
            if DEBUG:
                print(E)
            break
        except UnexpectedCharacters as E:
            print("DB_2020-15127> Syntax error")
            if DEBUG:
                print(E)
            break
        except UnexpectedEOF as E:
            print("DB_2020-15127> Syntax error")
            if DEBUG:
                print(E)
            break
        except UnexpectedInput as E:
            print("DB_2020-15127> Syntax error")
            if DEBUG:
                print(E)
            break

metadata.close()





