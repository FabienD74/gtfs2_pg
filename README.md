# gtfs2_pg

This Version is an attempt to modify GFTS2 ( link: https://github.com/vingerha/gtfs2/ )  to support Postgresql and test some features here and there....

Not stable, changed everyday, adding feature, changing code,...

Main feature are:
=>  able to maintain some kind of datasources. I created in a tiny table with 2 columns: db_id (integer) and connection string(text). Connection string in SQLAlchemy format 
=> create sensors using db_id (setup above)
=> be able to change db_id of a sensor to use another connection string, thus another DB ( Prod/test/dev... it's up to you to create those)
=> create a "Master sensor" which is responsible of DB operations, and may be to perform some additionnal tests ( min date, max date, number of rows on each table,)
=> master sensor should also maintain DB status ( "Online", "Offline", "Maintenance in progress insert in table xxxxx row yyyyyy")
=> user sensors DO NOT update/delete/insert

That's already a lot of changes everychere!
