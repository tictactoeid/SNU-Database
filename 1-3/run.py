import sys, os
import time

from lark import Lark, Transformer, UnexpectedToken, UnexpectedCharacters, UnexpectedInput, UnexpectedEOF, Tree, Token
from berkeleydb import db

import ThreeValuedLogic
from BooleanFactor import *
from ThreeValuedLogic import *

DEBUG = False # TODO: make it False

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

def operate_comparison_predicate(predicate, operand_1_value, operand_2_value):
    # 객체로 저장이 완료된 comparison predicate를
    # TRUE, FALSE, UNKNOWN 중 어느 것인지 계산
    if operand_1_value is None or operand_2_value is None:
        # null exception
        # 이후 null 고려할 필요 없음
        result = ThreeValuedLogic.UNKNOWN
    elif operand_1_value == ThreeValuedLogic.NULL or operand_2_value == ThreeValuedLogic.NULL:
        result = ThreeValuedLogic.UNKNOWN  # TODO: 이거 지워도 되나
    else:
        # TODO: consider datetime comparison: predicate의 value는 time_struct로, DB file에는 str로 저장됨
        # if predicate.operand_1.comparable_value_type == "date":
        #    operand_1_value = time.strptime(operand_1_value, "%Y-%m-%d")
        #    operand_2_value = time.strptime(operand_2_value, "%Y-%m-%d")
        if predicate.operand_1.comparable_value_type == "date" or predicate.operand_2.comparable_value_type == "date":
            # TODO

            if predicate.operand_1.type == ComparisonOperand.COLUMN_NAME:  # "date" type value - saved as str
                operand_1_value = time.strptime(operand_1_value, "%Y-%m-%d")
            if predicate.operand_2.type == ComparisonOperand.COLUMN_NAME:
                operand_2_value = time.strptime(operand_2_value, "%Y-%m-%d")
        operator = predicate.operator

        if operator == ComparisonPredicate.LESSTHAN:
            if operand_1_value < operand_2_value:
                result = ThreeValuedLogic.TRUE
            else:
                result = ThreeValuedLogic.FALSE
        elif operator == ComparisonPredicate.LESSEQUAL:
            if operand_1_value <= operand_2_value:
                result = ThreeValuedLogic.TRUE
            else:
                result = ThreeValuedLogic.FALSE
        elif operator == ComparisonPredicate.GREATERTHAN:
            if operand_1_value > operand_2_value:
                result = ThreeValuedLogic.TRUE
            else:
                result = ThreeValuedLogic.FALSE
        elif operator == ComparisonPredicate.GREATEREQUAL:
            if operand_1_value >= operand_2_value:
                result = ThreeValuedLogic.TRUE
            else:
                result = ThreeValuedLogic.FALSE
        elif operator == ComparisonPredicate.EQUAL:
            if operand_1_value == operand_2_value:
                result = ThreeValuedLogic.TRUE
            else:
                result = ThreeValuedLogic.FALSE
        elif operator == ComparisonPredicate.NOTEQUAL:
            if operand_1_value != operand_2_value:
                result = ThreeValuedLogic.TRUE
            else:
                result = ThreeValuedLogic.FALSE
        else:
            if DEBUG:
                print("operator: ", end='')
                print(operator)
                raise ("operator err")
    return result

def operate_null_predicate(predicate, operand_value):
    if predicate.is_not_null:
        if operand_value is not None:  # TODO: null이 어떻게 저장되지?
            result = ThreeValuedLogic.TRUE
        else:
            result = ThreeValuedLogic.FALSE
    else:
        if operand_value is None:
            result = ThreeValuedLogic.TRUE
        else:
            result = ThreeValuedLogic.FALSE
    return result

def get_operand_value_comparison_predicate(predicate, tables, tuples, table_cnt):
    # tables: table 이름 list
    # tuples: 각 table의 current tuple
    # table은 1~3개

    if predicate.operand_1.type == ComparisonOperand.COLUMN_NAME:
        if table_cnt == 1: # opnd.table_name may be None
            operand_1_value = tuples[0][predicate.operand_1.column_name]
        elif predicate.operand_1.table_name == tables[0]:
            operand_1_value = tuples[0][predicate.operand_1.column_name]
        elif predicate.operand_1.table_name == tables[1]:
            operand_1_value = tuples[1][predicate.operand_1.column_name]
        elif table_cnt > 2 and predicate.operand_1.table_name == tables[2]:
            operand_1_value = tuples[2][predicate.operand_1.column_name]
        else:
            raise ("operand_1 table not exists")
    else:
        operand_1_value = predicate.operand_1.value

    if predicate.operand_2.type == ComparisonOperand.COLUMN_NAME:
        if table_cnt == 1:
            operand_2_value = tuples[0][predicate.operand_2.column_name]
        elif predicate.operand_2.table_name == tables[0]:
            operand_2_value = tuples[0][predicate.operand_2.column_name]
        elif predicate.operand_2.table_name == tables[1]:
            operand_2_value = tuples[1][predicate.operand_2.column_name]
        elif table_cnt > 2 and predicate.operand_2.table_name == tables[2]:
            operand_2_value = tuples[2][predicate.operand_2.column_name]
        else:
            raise ("operand_2 table not exists")
    else:
        operand_2_value = predicate.operand_2.value

    return operand_1_value, operand_2_value
def get_operand_value_null_predicate(predicate, tables, tuples, table_cnt):
    # TODO
    if table_cnt == 1:
        operand_value = tuples[0][predicate.column_name]
    elif predicate.table_name == tables[0]:
        operand_value = tuples[0][predicate.column_name]
    elif predicate.table_name == tables[1]:
        operand_value = tuples[1][predicate.column_name]
    elif table_cnt > 2 and predicate.table_name == tables[2]:
        operand_value = tuples[2][predicate.column_name]
    else:
        raise ("operand table not exists")
    return operand_value

def operate_where_clause(where_clause_result):
    # true, false, unknown, and, or, not으로 이루어진 where clause list를 입력받아
    # 연산자 중 depth가 높은 것을 계산하고 list 수정
    # 반복하여 하나만 남으면 그것이 최종 결과이다.
    if DEBUG:
        print(where_clause_result)
    while len(where_clause_result) > 1:
        for idx in range(len(where_clause_result)):
            operator = where_clause_result[idx]
            if isinstance(operator, BooleanOperator):
                max_depth_idx = idx  # init
                break
        for idx in range(len(where_clause_result)):
            operator = where_clause_result[idx]
            if not isinstance(operator, BooleanOperator):
                continue
            if operator.depth > where_clause_result[max_depth_idx].depth:
                max_depth_idx = idx
            # the highest priority를 가진 operator를 찾고, 해당 operator를 연산함

        if where_clause_result[max_depth_idx].type == BooleanOperator.NOT:
            operand = where_clause_result[max_depth_idx + 1]
            result = ThreeValuedLogic.ThreeValuedNOT(operand)
            where_clause_result[max_depth_idx] = result
            where_clause_result.pop(max_depth_idx + 1)

        elif where_clause_result[max_depth_idx].type == BooleanOperator.AND:
            operand_1 = where_clause_result[max_depth_idx - 1]
            operand_2 = where_clause_result[max_depth_idx + 1]
            result = ThreeValuedLogic.ThreeValuedAND(operand_1, operand_2)
            where_clause_result[max_depth_idx] = result
            where_clause_result.pop(max_depth_idx + 1)
            where_clause_result.pop(max_depth_idx - 1)

        elif where_clause_result[max_depth_idx].type == BooleanOperator.OR:
            operand_1 = where_clause_result[max_depth_idx - 1]
            operand_2 = where_clause_result[max_depth_idx + 1]
            result = ThreeValuedLogic.ThreeValuedOR(operand_1, operand_2)
            where_clause_result[max_depth_idx] = result
            where_clause_result.pop(max_depth_idx + 1)
            where_clause_result.pop(max_depth_idx - 1)

        if DEBUG:
            for i in where_clause_result:
                print(i, end=' ')
            print()
    if DEBUG:
        print(where_clause_result)
    return where_clause_result[0]

class SQLTransformer(Transformer): # lark transformer class
    # TODO: null의 insert, delete, where clause
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
            values.append(x.children[0]) # TODO: 여기에 왜 lower()가 ?
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
            if value_type_current == "null": #str(value_current).lower() == "null":
                if not nullable_current:
                    # InsertColumnNonNullableError(#colName)
                    print("DB_2020-15127> Insertion has failed: \'" + col + "\' is not nullable")
                    return
                else:
                    values_dict[col] = None # save None as null
                    # TODO: select에서 null check
            elif type_current == "char":
                    if len(value_current) > length_current:
                        values_dict[col] = value_current[1:length_current+1] # truncate
                    else:
                        values_dict[col] = value_current[1:-1] # remove ''
            elif type_current == "int":
                values_dict[col] = int(values_dict[col])
            #else: # type_current == "date"
            #    values_dict[col] = time.strptime(values_dict[col], "%Y-%m-%d")

            # date형을 time_struct로 저장하면 추후 db에 저장된 tuple을 읽어 eval()할 때 error가 발생
            # 어차피 각 column의 type을 따로 저장하고 있으므로, 실제 table에는 모두 string형으로 저장하도록 함
            # 다만, where clause에서 이를 주의해야 함.
            if DEBUG:
                print(type(values_dict[col]))

        # Optional #1: InsertDuplicatePrimaryKeyError
        # Primary Key가 여러 개인 경우, 각 tuple의 모든 PK가 같아야 두 PK가 중복된 것으로 본다
        cursor = table_db.cursor()
        while x := cursor.next():
            key, value = x
            tuple_dict = eval(value)
            duplicated_pk = []
            for col in pk_list:
                value_current = values_dict[col]
                value_compare = tuple_dict[col]
                if value_current == value_compare: # 현재 column의 pk가 같음
                    duplicated_pk.append(True)
                else:
                    duplicated_pk.append(False) # 다름
            if pk_list and (False not in duplicated_pk): # pk의 모든 조합이 같음
                # InsertDuplicatePrimaryKeyError
                print("DB_2020-15127> Insertion has failed: Primary key duplication")
                return

        # Optional #2: InsertReferentialIntegrityError
            # TODO: multiple fk의 경우 같은 tuple에서 가져와야지 일부분씩 가져오면 안 됨
        fk_ref_table_list = set()
        for col_dict in columns:
            if col_dict["column_name"] in fk_list:
                fk_ref_table_list.add(col_dict["fk_ref_table"])

        for fk_ref_table in fk_ref_table_list: # 같은 table을 여러 fk가 참조하는 경우, 해당 fk들의 "조합" 이 referencing table에 존재해야 함.
            fk_ref_column_list = []
            for col_dict in columns:
                if col_dict["column_name"] in fk_list:
                    if fk_ref_table == col_dict["fk_ref_table"]:
                        fk_ref_column_list.append([col_dict["column_name"], col_dict["fk_ref_column"]])
                    else:
                        continue # 서로 다른 table을 참조하는 fk의 경우, 각각의 value가 referencing table에 존재는 해야 하나 "조합"될 필요는 없음.
                        # 현재 확인 중인 referencing table과 현재 column(fk)가 관련없으므로 일단 넘어감
            ref_table_db = db.DB()
            ref_table_db.open('./DB/' + fk_ref_table + '.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
            ref_cursor = ref_table_db.cursor()
            #flag = False

            flag = False
            while y := ref_cursor.next():
                ref_integrity = []
                ref_key, ref_value = y
                ref_tuple_dict = eval(ref_value)
                for fk_col_pair in fk_ref_column_list:
                    value_current = values_dict[fk_col_pair[0]]
                    value_compare = ref_tuple_dict[fk_col_pair[1]]
                    if value_current == value_compare:
                        ref_integrity.append(True)
                    else:
                        ref_integrity.append(False)
                if False not in ref_integrity: # 특정 table에 대한 fk의 모든 조합이 pk 조합과 같음
                    flag = True
                    break
            ref_table_db.close()
            if fk_list and (not flag):
                # InsertReferentialIntegrityError
                print("DB_2020-15127> Insertion has failed: Referential integrity violation")
                return

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
                # TODO: Optional #3, DeleteReferentialIntegrityPassed
                table_db.delete(key)
                cnt += 1
            print(f"DB_2020-15127> \'{cnt}\' row(s) are deleted")
            return

        # where clause 정보를 저장
        where_clause = []
        where_iter = find_data_multiple(items[3], ["and_op", "or_op", "not_op", "comparison_predicate", "null_predicate"])
        for i in where_iter:
            if i.data == "comparison_predicate":  # comp_operand comp_op comp_operand
                operator = i.children[1].children[0].value
                if i.children[0].children[0]:
                    if i.children[0].children[0].data == "table_name":
                        operand_1 = ComparisonOperand(type="column_name", table_name=i.children[0].children[0].children[0].value, column_name=i.children[0].children[1].children[0].value)
                    elif i.children[0].children[0].data == "comparable_value":
                        operand_1 = ComparisonOperand(type="comparable_value", value=i.children[0].children[0].children[0].value, comparable_value_type=i.children[0].children[0].children[0].type)
                else: # i.children[0].children[0] is None
                    # table_name has omitted
                    operand_1 = ComparisonOperand(type="column_name", table_name=None, column_name=i.children[0].children[1].children[0].value)

                if i.children[2].children[0]:
                    if i.children[2].children[0].data == "table_name":
                        operand_2 = ComparisonOperand(type="column_name", table_name=i.children[2].children[0].children[0].value, column_name=i.children[2].children[1].children[0].value)
                    elif i.children[2].children[0].data == "comparable_value":
                        operand_2 = ComparisonOperand(type="comparable_value", value=i.children[2].children[0].children[0].value, comparable_value_type=i.children[2].children[0].children[0].type)
                else:
                    operand_2 = ComparisonOperand(type="column_name", table_name=None, column_name=i.children[2].children[1].children[0].value)
                if DEBUG:
                    print(operand_1.value, end=' ')
                    print(type(operand_1.value))
                    print(operand_2.value, end=' ')
                    print(type(operand_2.value))

                if operand_1.type == "column_name":
                    if operand_1.table_name is not None and operand_1.table_name != table_name:
                        # WhereTableNotSpecified
                        print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                        return
                    if operand_1.column_name not in column_list:
                        # WhereColumnNotExist
                        print("DB_2020-15127> Where clause trying to reference non existing column")
                        return
                    for col_dict in columns:
                        if col_dict["column_name"] == operand_1.column_name:
                            operand_1.set_comparable_value_type(col_dict["type"])
                            break

                if operand_2.type == "column_name":
                    if operand_2.table_name is not None and operand_2.table_name != table_name:
                        # WhereTableNotSpecified
                        print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                        return
                    if operand_2.column_name not in column_list:
                        # WhereColumnNotExist
                        print("DB_2020-15127> Where clause trying to reference non existing column")
                        return
                    for col_dict in columns:
                        if col_dict["column_name"] == operand_2.column_name:
                            operand_2.set_comparable_value_type(col_dict["type"])
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
                    table_name_current = i.children[0].children[0].value.lower()
                    if table_name_current != table_name:
                        # WhereTableNotSpecified
                        print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                        return
                else: # [table_name] has omitted
                    table_name_current = None

                if column_name_current not in column_list:
                    # WhereColumnNotExist
                    print("DB_2020-15127> Where clause trying to reference non existing column")
                    return

                if i.children[2].children[1]:
                    is_not_null = True # is NOT null
                else:
                    is_not_null = False # is null

                where_clause.append(NullPredicate(column_name=column_name_current, table_name=table_name_current, is_not_null=is_not_null))

            elif i.data == "and_op":
                where_clause.append(BooleanOperator(type="and", depth=get_depth(items[3], i)))
            elif i.data == "or_op":
                where_clause.append(BooleanOperator(type="or", depth=get_depth(items[3], i)))
            elif i.data == "not_op":
                where_clause.append(BooleanOperator(type="not", depth=get_depth(items[3], i)))
            # lark tree에서 depth가 깊은 operator의 우선순위가 더 높다.
        # where clause 저장 끝

        #max = 0
        # 각 tuple에 대해 where clause result를 계산
        cursor = table_db.cursor()
        cnt = 0
        while x := cursor.next():
            key, value = x
            if DEBUG:
                print(value)
            tuple_dict = eval(value) # current tuple
            where_clause_result = []
            for idx in range(len(where_clause)): # 각 tuple에 대해 각 boolean term 게산. True/False/Unknown
                predicate = where_clause[idx]
                if isinstance(predicate, ComparisonPredicate):
                    # TODO
                    operand_1_value, operand_2_value = get_operand_value_comparison_predicate(predicate, [table_name], [tuple_dict], 1)
                    result = operate_comparison_predicate(predicate, operand_1_value, operand_2_value)

                elif isinstance(predicate, NullPredicate):
                    operand_value = tuple_dict[predicate.column_name]
                    result = operate_null_predicate(predicate, operand_value)
                else:
                    result = predicate
                where_clause_result.append(result)

            if DEBUG:
                print("------parsed where clause------")
                for i in where_clause:
                    print(i, end= ' ')
                print("\n------where clause result------")
                for i in where_clause_result:
                    print(i, end=' ')
                print()

            final = operate_where_clause(where_clause_result)
            if final == ThreeValuedLogic.TRUE:
                # TODO: Optional #3, DeleteReferentialIntegrityPassed
                # Optional #3





                # Optional #3 end



                table_db.delete(key)
                cnt += 1
            # where clause 연산 끝
        print(f"DB_2020-15127> \'{cnt}\' row(s) are deleted")
        return

    def select_query(self, items):
        table_list = [] # maximum three tables
        table_schema_list = []
        table_db_list = []
        column_list = []
        for i in items[2].find_data("referred_table"):
            table_name = i.children[0].children[0].value
            table_list.append(table_name)
            table_schema = metadata.get(table_name.encode())
            if table_schema is None:
                # SelectTableExistenceError(#tableName)
                print("DB_2020-15127> Selection has failed: \'"+table_name+"\' does not exist")
                return
            table_schema = eval(table_schema.decode())
            table_schema_list.append(table_schema)
            table_db = db.DB()
            table_db.open('./DB/' + table_name + '.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
            table_db_list.append(table_db)
            current_column_list = []
            for col_dict in table_schema["columns"]:
                current_column_list.append(col_dict["column_name"])
            column_list.append(current_column_list)

        selected_column_list = []
        if items[1].children == []:
            # select *
            for table_schema in table_schema_list:
                for col_dict in table_schema["columns"]:
                    table_name = table_schema["table_name"]
                    column_name = col_dict["column_name"]
                    selected_col_dict = {"table_name": table_name, "column_name": column_name,
                                         "table_name_called": False}
                    selected_column_list.append(selected_col_dict)
            for col_1 in selected_column_list:
                for col_2 in selected_column_list:
                    if col_1["table_name"] != col_2["table_name"] and col_1["column_name"] == col_2["column_name"]:
                        col_1["table_name_called"] = True
                        col_2["table_name_called"] = True
        else:
            for i in items[1].find_data("selected_column"):
                column_name = i.children[1].children[0].value
                if i.children[0] is not None:
                    table_name = i.children[0].children[0].value
                    table_name_called = True
                else:
                    table_name = None
                    table_name_called = False
                    for table_schema in table_schema_list:
                        for col_dict in table_schema["columns"]:
                            if column_name == col_dict["column_name"]:
                                if table_name is None:
                                    table_name = table_schema["table_name"]
                                else:
                                    # SelectColumnResolveError(#colName)
                                    # 모호한 경우 - column 이름이 중복되었으나 table name이 명시되지 않은 경우
                                    print("DB_2020-15127> Selection has failed: fail to resolve \'" + column_name + "\'")
                                    return
                    if table_name is None:
                        # SelectColumnResolveError(#colName)
                        # 존재하지 않는 column
                        print("DB_2020-15127> Selection has failed: fail to resolve \'" + column_name + "\'")
                        return
                selected_col_dict = {"table_name": table_name, "column_name": column_name,
                                     "table_name_called": table_name_called}
                selected_column_list.append(selected_col_dict)
                # table_name_called : query가 table_name.column_name의 형태인지 column_name의 형태인지 저장. 출력 시 필요

        # TODO: where clause 수정 start
        where_clause = []
        where_iter = find_data_multiple(items[2], ["and_op", "or_op", "not_op", "comparison_predicate", "null_predicate"])
        for i in where_iter:
            if i.data == "comparison_predicate":  # comp_operand comp_op comp_operand
                operator = i.children[1].children[0].value
                if i.children[0].children[0]:
                    if i.children[0].children[0].data == "table_name":
                        operand_1 = ComparisonOperand(type="column_name", table_name=i.children[0].children[0].children[0].value, column_name=i.children[0].children[1].children[0].value)
                    elif i.children[0].children[0].data == "comparable_value":
                        operand_1 = ComparisonOperand(type="comparable_value", value=i.children[0].children[0].children[0].value, comparable_value_type=i.children[0].children[0].children[0].type)
                else: # i.children[0].children[0] is None
                    # table_name has omitted
                    operand_1 = ComparisonOperand(type="column_name", table_name=None, column_name=i.children[0].children[1].children[0].value)

                if i.children[2].children[0]:
                    if i.children[2].children[0].data == "table_name":
                        operand_2 = ComparisonOperand(type="column_name", table_name=i.children[2].children[0].children[0].value, column_name=i.children[2].children[1].children[0].value)
                    elif i.children[2].children[0].data == "comparable_value":
                        operand_2 = ComparisonOperand(type="comparable_value", value=i.children[2].children[0].children[0].value, comparable_value_type=i.children[2].children[0].children[0].type)
                else:
                    operand_2 = ComparisonOperand(type="column_name", table_name=None, column_name=i.children[2].children[1].children[0].value)
                if DEBUG:
                    print(operand_1.value, end=' ')
                    print(type(operand_1.value))
                    print(operand_2.value, end=' ')
                    print(type(operand_2.value))

                if operand_1.type == "column_name" and operand_1.table_name is None:
                    table_name = None
                    for table_schema in table_schema_list:
                        for col_dict in table_schema["columns"]:
                            if operand_1.column_name == col_dict["column_name"]:
                                if table_name is None:
                                    table_name = table_schema["table_name"]
                                    operand_1.set_comparable_value_type(col_dict["type"])
                                else:
                                    # WhereAmbiguousReference
                                    # 모호한 경우 - column 이름이 중복되었으나 table name이 명시되지 않은 경우
                                    print("DB_2020-15127> Where clause contains ambiguous reference")
                                    return
                    if table_name is None:
                        # WhereColumnNotExist
                        print("DB_2020-15127> Where clause trying to reference non existing column")
                        return
                    operand_1.table_name = table_name
                elif operand_1.type == "column_name":
                    idx = 0
                    for i in range(len(table_list)):
                        if table_list[i] == operand_1.table_name:
                         idx = i
                         break
                    for col_dict in table_schema_list[i]["columns"]:
                        if operand_1.column_name == col_dict["column_name"]:
                            operand_1.set_comparable_value_type(col_dict["type"])
                            break

                if operand_1.table_name is not None and operand_1.table_name not in table_list:
                    # WhereTableNotSpecified
                    print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                    return

                if operand_2.type == "column_name" and operand_2.table_name is None:
                    table_name = None
                    for table_schema in table_schema_list:
                        for col_dict in table_schema["columns"]:
                            if operand_2.column_name == col_dict["column_name"]:
                                if table_name is None:
                                    table_name = table_schema["table_name"]
                                    operand_2.set_comparable_value_type(col_dict["type"])
                                else:
                                    # WhereAmbiguousReference
                                    # 모호한 경우 - column 이름이 중복되었으나 table name이 명시되지 않은 경우
                                    print("DB_2020-15127> Where clause contains ambiguous reference")
                                    return
                    if table_name is None:
                        # WhereColumnNotExist
                        print("DB_2020-15127> Where clause trying to reference non existing column")
                        return
                    operand_2.table_name = table_name

                elif operand_2.type == "column_name":
                    idx = 0
                    for i in range(len(table_list)):
                        if table_list[i] == operand_2.table_name:
                         idx = i
                         break
                    for col_dict in table_schema_list[i]["columns"]:
                        if operand_2.column_name == col_dict["column_name"]:
                            operand_2.set_comparable_value_type(col_dict["type"])
                            break

                if operand_2.table_name is not None and operand_2.table_name not in table_list:
                    # WhereTableNotSpecified
                    print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                    return

                if operand_1.comparable_value_type == "null" or operand_2.comparable_value_type == "null":
                    pass
                elif operand_1.comparable_value_type != operand_2.comparable_value_type:
                    # WhereIncomparableError
                    #print(operand_1.comparable_value_type)
                    #print(operand_2.comparable_value_type)
                    print("DB_2020-15127> Where clause trying to compare incomparable values")
                    return
                where_clause.append(ComparisonPredicate(operand_1=operand_1, operator=operator, operand_2=operand_2))

            elif i.data == "null_predicate":
                # [table_name "."] column_name null_operation
                # column_name, table_name = None, is_not_null = False
                column_name_current = i.children[1].children[0].value
                if i.children[0]:
                    table_name_current = i.children[0].children[0].value.lower()
                    if table_name_current not in table_list:
                        # WhereTableNotSpecified
                        print("DB_2020-15127> Where clause trying to reference tables which are not specified")
                        return
                else: # [table_name] has omitted
                    table_name_current = None
                    for table_schema in table_schema_list:
                        for col_dict in table_schema["columns"]:
                            if column_name_current == col_dict["column_name"]:
                                if table_name_current is None:
                                    table_name_current = table_schema["table_name"]
                                else:
                                    # WhereAmbiguousReference
                                    print("DB_2020-15127> Where clause contains ambiguous reference")
                                    return
                    if table_name_current is None:
                        # WhereColumnNotExist
                        print("DB_2020-15127> Where clause trying to reference non existing column")
                        return

                if i.children[2].children[1]: # is NOT null
                    is_not_null = True
                else: # is null
                    is_not_null = False

                where_clause.append(NullPredicate(column_name=column_name_current, table_name=table_name_current, is_not_null=is_not_null))

            elif i.data == "and_op":
                where_clause.append(BooleanOperator(type="and", depth=get_depth(items[2], i)))
            elif i.data == "or_op":
                where_clause.append(BooleanOperator(type="or", depth=get_depth(items[2], i)))
            elif i.data == "not_op":
                where_clause.append(BooleanOperator(type="not", depth=get_depth(items[2], i)))

            # TODO: where clause end

        column_count = len(selected_column_list)
        for i in range(column_count):
            print("+", end='')
            print('-' * 20, end='')
        print('+')
        strFormat = '| %-18s '
        for selected_col_dict in selected_column_list:
            if selected_col_dict["table_name_called"]:
                col_print = selected_col_dict["table_name"] + "." + selected_col_dict["column_name"]
            else:
                col_print = selected_col_dict["column_name"]
            print(strFormat % col_print.upper(), end='')
        print('|')
        for i in range(column_count):
            print("+", end='')
            print('-' * 20, end='')
        print('+')

        table_count = len(table_list)
        if table_count == 3:
            cursor_0 = table_db_list[0].cursor()
            while x := cursor_0.next():
                cursor_1 = table_db_list[1].cursor()
                while y := cursor_1.next():
                    cursor_2 = table_db_list[2].cursor()
                    while z := cursor_2.next():
                        # TODO: where clause result
                        key_0, value_0 = x
                        key_1, value_1 = y
                        key_2, value_2 = z
                        tuple_dict_0 = eval(value_0)
                        tuple_dict_1 = eval(value_1)
                        tuple_dict_2 = eval(value_2) # current tuples

                        tuples = [tuple_dict_0, tuple_dict_1, tuple_dict_2]
                        where_clause_result = []
                        for idx in range(len(where_clause)):  # 각 tuple에 대해 각 boolean term 게산. True/False/Unknown
                            predicate = where_clause[idx]
                            if isinstance(predicate, ComparisonPredicate):
                                operand_1_value, operand_2_value = get_operand_value_comparison_predicate(predicate, table_list, tuples, table_count) # table_count == 3
                                result = operate_comparison_predicate(predicate, operand_1_value, operand_2_value)
                            elif isinstance(predicate, NullPredicate):
                                operand_value = get_operand_value_null_predicate(predicate, table_list, tuples, table_count)
                                result = operate_null_predicate(predicate, operand_value)
                            else:
                                result = predicate
                            where_clause_result.append(result)

                        if DEBUG:
                            print("------parsed where clause------")
                            for i in where_clause:
                                print(i, end=' ')
                            print("\n------where clause result------")
                            for i in where_clause_result:
                                print(i, end=' ')
                            print()
                        if not where_clause_result: # where clause has omitted at query
                            final = ThreeValuedLogic.TRUE
                        else:
                            final = operate_where_clause(where_clause_result)
                        if final == ThreeValuedLogic.TRUE:
                            # TODO: print this
                            for selected_col_dict in selected_column_list:
                                col = selected_col_dict["column_name"]
                                if selected_col_dict["table_name"] == table_list[0]:
                                    print_value = tuples[0][col]
                                elif selected_col_dict["table_name"] == table_list[1]:
                                    print_value = tuples[1][col]
                                else:
                                    print_value = tuples[2][col]

                                if print_value is None:
                                    print_value = 'null'
                                elif isinstance(print_value, time.struct_time):
                                    print_value = time.strftime('%Y-%m-%d', print_value)
                                print(strFormat % print_value, end='')
                            print('|')
        elif table_count == 2:
            cursor_0 = table_db_list[0].cursor()
            while x := cursor_0.next():
                cursor_1 = table_db_list[1].cursor()
                while y := cursor_1.next():
                    key_0, value_0 = x
                    key_1, value_1 = y
                    tuple_dict_0 = eval(value_0)
                    tuple_dict_1 = eval(value_1)

                    tuples = [tuple_dict_0, tuple_dict_1]
                    where_clause_result = []
                    for idx in range(len(where_clause)):  # 각 tuple에 대해 각 boolean term 게산. True/False/Unknown
                        predicate = where_clause[idx]
                        if isinstance(predicate, ComparisonPredicate):
                            operand_1_value, operand_2_value = get_operand_value_comparison_predicate(predicate, table_list, tuples, table_count) # table_count == 2
                            result = operate_comparison_predicate(predicate, operand_1_value, operand_2_value)

                        elif isinstance(predicate, NullPredicate):
                            operand_value = get_operand_value_null_predicate(predicate, table_list, tuples, table_count)
                            result = operate_null_predicate(predicate, operand_value)
                        else:
                            result = predicate
                        where_clause_result.append(result)

                    if DEBUG:
                        print("------parsed where clause------")
                        for i in where_clause:
                            print(i, end=' ')
                        print("\n------where clause result------")
                        for i in where_clause_result:
                            print(i, end=' ')
                        print()

                    if not where_clause_result:  # where clause has omitted at query
                        final = ThreeValuedLogic.TRUE
                    else:
                        final = operate_where_clause(where_clause_result)
                    if final == ThreeValuedLogic.TRUE:
                        for selected_col_dict in selected_column_list:
                            col = selected_col_dict["column_name"]
                            if selected_col_dict["table_name"] == table_list[0]:
                                print_value = tuples[0][col]
                            else:
                                print_value = tuples[1][col]

                            if print_value is None:
                                print_value = 'null'
                            elif isinstance(print_value, time.struct_time):
                                print_value = time.strftime('%Y-%m-%d', print_value)
                            print(strFormat % print_value, end='')
                        print('|')

        else: # table_count == 1
            cursor_0 = table_db_list[0].cursor()
            while x := cursor_0.next():
                key_0, value_0 = x
                tuple_dict_0 = eval(value_0)

                tuples = [tuple_dict_0]
                where_clause_result = []
                for idx in range(len(where_clause)):  # 각 tuple에 대해 각 boolean term 게산. True/False/Unknown
                    predicate = where_clause[idx]
                    if isinstance(predicate, ComparisonPredicate):
                        operand_1_value, operand_2_value = get_operand_value_comparison_predicate(predicate, table_list, tuples, table_count) # table_count == 1
                        result = operate_comparison_predicate(predicate, operand_1_value, operand_2_value)

                    elif isinstance(predicate, NullPredicate):
                        operand_value = get_operand_value_null_predicate(predicate, table_list, tuples, table_count)
                        result = operate_null_predicate(predicate, operand_value)
                    else:
                        result = predicate
                    where_clause_result.append(result)

                if DEBUG:
                    print("------parsed where clause------")
                    for i in where_clause:
                        print(i, end=' ')
                    print("\n------where clause result------")
                    for i in where_clause_result:
                        print(i, end=' ')
                    print()

                if not where_clause_result:  # where clause has omitted at query
                    final = ThreeValuedLogic.TRUE
                else:
                    final = operate_where_clause(where_clause_result)
                if final == ThreeValuedLogic.TRUE:
                    for selected_col_dict in selected_column_list:
                        col = selected_col_dict["column_name"]
                        print_value = tuples[0][col]
                        if print_value is None:
                            print_value = 'null'
                        elif isinstance(print_value, time.struct_time):
                            print_value = time.strftime('%Y-%m-%d', print_value)
                        print(strFormat % print_value, end='')
                    print('|')


        for i in range(column_count):
            print("+", end='')
            print('-' * 20, end='')
        print('+')

        for table_db in table_db_list:
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





