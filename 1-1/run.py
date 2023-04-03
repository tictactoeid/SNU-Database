import sys

from lark import Lark, Transformer, UnexpectedToken

with open("grammar.lark") as grammar: # grammer.lark open
    sql_parser = Lark(grammar.read(), start="command", lexer="basic")
    # sql parser

class SQLTransformer(Transformer): # lark transformer class
    def create_table_query(self, items): # called when 'CREATE TABLE' query requested well
        print("DB_2020-15127> \'CREATE TABLE\' requested")

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
    #def exit(self, items):
        # TODO: 여기서 sys.exit()를 call하는 것이 program을 terminate하지 않음

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
            # print(E)
            break


