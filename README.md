# gtfs2_pg

<<<<<<< HEAD
This version is an attempt to modify GFTS2 ( link: https://github.com/vingerha/gtfs2/ )  to support Postgresql and test some features here and there....

Not stable, changed everyday, adding feature, changing code,...

Main feature are (...should be) :

- able to maintain some kind of datasources. I created in a tiny table with 2 columns: db_id (integer) and connection string(text). Connection string in SQLAlchemy format (75% done)
- create sensors using db_id (setup above) (50% done for existing sensor)
- able to change db_id of a sensor to use another DB ( Prod/test/dev... it's up to you to create those But you need at lease ONE :)  ) (DONE)
- create automatically a "Master sensor" (controller, main, ... you name it) which is responsible of DB operations, and perform some additionnal tests ( min date ?, max date?, number of rows on each table?,...) (DONE 0%)
- master sensor  should also maintain DB status ( "Online", "Offline", "Online but xxxxxx" ,........... "Maintenance in progress insert in table xxxxx row yyyyyy" ?) (DONE 0%)
- user sensors  DO NOT update/delete/insert (if status above is strictly equal to "Online") ... (DONE 0%)

That's already a lot of changes everywhere!
=======
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
>>>>>>> f124a38f76f390fb0756726f5b60102dedc21488
