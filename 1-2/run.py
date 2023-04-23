import sys
from lark import Lark, Transformer, UnexpectedToken
from berkeleydb import db

with open("grammar.lark") as grammar:
    sql_parser = Lark(grammar.read(), start="command", lexer="basic")

metadata = db.DB()
metadata.open('./DB/metadata.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)
# table schema file

class SQLTransformer(Transformer): # lark transformer class
    def create_table_query(self, items): # called when 'CREATE TABLE' query requested well
        # CREATE TABLE table_name table_element_list
        # TODO: 예외 처리

        table_name = items[2].children[0].lower()
        column_definition_iter = items[3].find_data("column_definition")
        table_constraint_definition_iter = items[3].find_data("table_constraint_definition")

        columns = []
        primary_key = None
        foreign_keys = []

        for j in table_constraint_definition_iter:
            if (j.children[0].children[0].lower() == "primary"):
                # PRIMARY KEY column_name_list
                for k in j.find_data("column_name"):
                    if (primary_key != None):
                        print("DB_2020-15127> Create table has failed: primary key definition is duplicated")
                        return
                    primary_key = k.children[0].lower()
            else:
                # FOREIGN KEY column_name_list REFERENCES table_name column_name_list
                fk_col_name = j.children[0].children[2].children[1].children[0].lower()
                ref_table_name = j.children[0].children[4].children[0].lower()
                ref_col_name = j.children[0].children[5].children[1].children[0].lower()
                # for k in j.find_data("table_name"):
                #    ref_table_name = k.children[0].lower()

                fk_dict = {fk_col_name : [ref_table_name, ref_col_name]}
                foreign_keys.append(fk_dict)

        for i in column_definition_iter:
            # column_name data_type [NOT NULL]
            column_name = i.children[0].children[0].lower()
            type = i.children[1].children[0].lower()

            if column_name == primary_key:
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
                    column_dict["fk_ref_table"] = fk_dict.get(column_name)[0]
                    column_dict["fk_ref_column"] = fk_dict.get(column_name)[1]
            columns.append(column_dict)

        fk_list = []
        for fk_dict in foreign_keys:
            fk_list += list(fk_dict.keys())

        table_schema = {
            "table_name" : table_name,
            "columns" : columns,
            "primary_key" : primary_key,
            "foreign_key" : fk_list
        }
        metadata.put(table_name.encode(), str(table_schema).encode())

        cursor = metadata.cursor() # TODO: delete this / for debug
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
        data += ' ' # se\nlect 등의 잘못된 줄 바꿈을 걸러내기 위해, line과 line 사이에 ' ' 삽입
        data += input() # ;가 입력되기 전까지는 prompt 출력 없이 계속 input만 받음
    querys = data.rstrip().split(';') # semicolon 기준으로 input을 split, whitespace를 제거
    for query in querys[0:-1]: # query list를 하나씩 순차 실행
        # ; 기준 split시 list의 마지막은 ''가 되므로 [0:-1]
        if query.strip() == "exit": # exit;
            sys.exit()
        try:
            output = sql_parser.parse(query + ';')
            transformer.transform(output) # insert_query() 등 적절한 function call이 자동으로 이루어짐
        except UnexpectedToken as E: # syntax error
            print("DB_2020-15127> Syntax error")
            break


#dbObject.close()
metadata.close()