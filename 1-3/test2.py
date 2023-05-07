from berkeleydb import db

metadata = db.DB()
metadata.open('./DB/metadata.db', dbtype=db.DB_HASH, flags=db.DB_DUP | db.DB_CREATE)

metadata.put(b"aaa", b"info")
metadata.put(b"aaa", b"info22")
metadata.put(b"aaa", b"info33")
metadata.put(b"aaa", b"info44")
#metadata.put(b"column_name123", b"info123")

cursor = metadata.cursor()
#cursor.set_range(b"column_name")
record = cursor.first()
print(record)
while record := cursor.next_dup():
    print(record)

metadata.close()

a = "test"
b = "hi"
c = "hello"
d = {a:{b:c}}
print(d)
print(d.get("test").keys())

e = {a:[b, c]}
print(e)
print(e.get(a)[0])
print(list(e.keys()))
print(bool("not"))