# hive 分区表创建示例

create table if not exists student(sn int, name string, age int, sex int)
partitioned by (createTime string)
row format delimited fields terminated by ','
stored as textfile;