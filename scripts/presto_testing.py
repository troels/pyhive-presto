from sqlalchemy.engine import create_engine
from sqlalchemy.dialects import registry

from presto.sqlalchemy_nzpresto import register_nzpresto_dialect

register_nzpresto_dialect()

engine = create_engine("nzpresto://localhost:32767/memory")

print(list(engine.execute("select * from b")))
engine.execute("drop table if exists b")

t = engine.begin()
t.conn.execute("drop table if exists b")
t.conn.execute("create table b (id integer)")
t.conn.execute("insert into b values (1)")
t.conn.execute("insert into b values (2)")
t.conn.execute("insert into b values (3)")
t.conn.execute("insert into b values (4)")
t.transaction.rollback()

t = engine.begin()
print(list(t.conn.execute("select * from b")))
