import sys

from lark import Lark, Transformer, UnexpectedToken

from berkeleydb import db

with open("grammar.lark") as grammar: # grammer.lark open
    sql_parser = Lark(grammar.read(), start="command", lexer="basic")
    # sql parser

#dbObject = db.DB()
#dbObject.open('dbFile.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)


class SQLTransformer(Transformer): # lark transformer class
    def create_table_query(self, items): # called when 'CREATE TABLE' query requested well
        # CREATE TABLE table_name table_element_list

        table_name = items[2].children[0].lower()
        column_definition_iter = items[3].find_data("column_definition")
        table_constraint_definition_iter = items[3].find_data("table_constraint_definition")
        columns = []

        #print(table_name)
        for i in column_definition_iter:
            column_name = i.children[0].children[0].lower()

            #print(i.children[2])
            #print(i.children[3])
            #print(i.pretty())
            if i.children[1].children[0].lower() == 'char':
                #print(i.children[1].children[2])
                pass
        #print(table_constraint_definition_iter)
        for j in table_constraint_definition_iter:
            if (j.children[0].children[0].lower() == 'foreign'):
                pass
                print(j.children[0].children[4].children[0].lower())
                #print(1)
                #print(j.pretty())
                #print(j.children[0].children[4].children[0]) # col name
                #print(j.children[0].children[2])
                #for k in j.find_data("column_name"):
                    #print(k)
                    #print(k.children[1:-1])
                #    pass
                #for m in j.find_data("table_name"):
                    #print(m.children[0])
                #    pass
            #else:
            #    for k in j.find_data("column_name"):
            #        print(k.children[0].lower())
    def drop_table_query(self, items):
        print(type(items[2].children[0].lower()))
        print("DB_2020-15127> \'DROP TABLE\' requested")

    def explain_query(self, items):
        table_name = items[1].children[0].lower()
        print(table_name)
        print("DB_2020-15127> \'EXPLAIN\' requested")
    def describe_query(self, items):
        print("DB_2020-15127> \'DESCRIBE\' requested")

    def desc_query(self, items):
        print("DB_2020-15127> \'DESC\' requested")

    def insert_query(self, items):
        print(items)
        print(items[3])

        #print(items[5].children[1])
        values_tree = items[5].children[1:-1]
        print(values_tree)
        for x in values_tree:
            print(x.children[0])
        print("DB_2020-15127> \'INSERT\' requested")

    def delete_query(self, items):
        print("DB_2020-15127> \'DELETE\' requested")
    def select_query(self, items):
        print(items[2].pretty())
        print(items[2].children[0].children[1].children[0].children[0].children[0])
        print("DB_2020-15127> \'SELECT\' requested")

    def show_tables_query(self, items):
        print("DB_2020-15127> \'SHOW TABLES\' requested")
    def update_query(self, items):
        print("DB_2020-15127> \'UPDATE\' requested")

transformer = SQLTransformer()




#output = sql_parser.parse('create table account (account_number int not null, branch_name char(15), primary key(account_number, branch_name), foreign key(branch_name) references table_name(ID), foreign key(account_number) references mytable(cols));')
#output = sql_parser.parse("drop table name;")
#output = sql_parser.parse("explain account;")
#output = sql_parser.parse("insert into table_name values(123, \"hello\", \"umm\");")
#output = sql_parser.parse("insert into table_name (id, name) values(123, \"hello\", \"umm\");")
#output = sql_parser.parse("insert into ogamdo (name, a_hae, children, id) values ('choi', 13, 2020-01-01, 5678);")
output = sql_parser.parse("select * from ogamdo;")
transformer.transform(output)
#strFormat = '%-10s%-10s%-10s\n'
#strOut = strFormat % ('abc','def','cgh')
#print(len("column_name		"))
#print(strOut)
print(len("----------------"))
print(len(" ACCOUNT_NUMBER "))
print(len(" A-101          "))