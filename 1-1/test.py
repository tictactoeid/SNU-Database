from lark import Lark, Transformer

with open("grammar.lark") as grammar:
    sql_parser = Lark(grammar.read(), start="command", lexer="basic")

class SQLTransformer(Transformer):

    def select_query(self, items):
        print("SELECT")

    def show_tables_query(self, items):
        print("SHOW TABLES")
    def drop_table_query(self, items):
        print("DROP TABLE")




query = "SELECT * FROM taaa ; SHOW TABLES; DROP TABLE ewrd; \n "

output = sql_parser.parse(query)
print(output.pretty())
querys = query.rstrip().split(';')
print(querys)
print(querys[0:-1])
print("---------- result -------------")
transformer = SQLTransformer()
for query in querys[0:-1]:
    output = sql_parser.parse(query+';')
    transformer.transform(output)
    print(output)