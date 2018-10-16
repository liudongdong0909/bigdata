hive> create table inner_table(key string);

hive> load data local inpath '/home/hadoop/hive_table_data/inner_table.dat' into table inner_table;

hive> select * from inner_table;
OK
donggua
huluwa
linqingxia
liudehua
chenglong
zhangmin
Time taken: 1.213 seconds, Fetched: 6 row(s)