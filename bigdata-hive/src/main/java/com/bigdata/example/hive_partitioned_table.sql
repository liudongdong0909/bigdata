语法：
CREATETABLEtmp_table#表名(titlestring,#字段名称字段类型minimum_biddouble,quantitybigint,have_invoicebigint)
COMMENT'注释：XXX'#表注释
PARTITIONEDBY(ptSTRING)#分区表字段（如果你文件非常之大的话，采用分区表可以快过滤出按分区字段划分的数据）
ROWFORMATDELIMITEDFIELDSTERMINATEDBY'\001'#字段是用什么分割开的
STORED AS SEQUENCEFILE; #用哪种方式存储数据，SEQUENCEFILE是hadoop自带的文件压缩格式

一些相关命令：
SHOW TABLES ; #查看所有的表
SHOW TABLES '*TMP*'; #支持模糊查询
HOW PARTITIONS TMP_TABLE; #查看表有哪些分区
DESCRIBE TMP_TABLE; #查看表结构

# hive 分区表创建示例

create table if not exists student(sn int, name string, age int, sex int)
partitioned by (createTime string)
row format delimited fields terminated by ','
stored as textfile;



  create table if not exists partition_table(rectime string, msisdn string)
  comment '分区表测试' partitioned by (daytime string, city string)
  row format delimited fields terminated by '\t'
  stored as textfile;

 hive> load data local inpath '/home/hadoop/hive_table_data/partition_table.dat' into table partition_table partition(daytime='2018-10-01', city='beijing');

hive>
    >
    > alter table partition_table add partition(daytime='2018-09-10', city='hangzhou');

hive> alter table partition_table drop partition (daytime='2018-09-10',city='hangzhou');
Dropped the partition daytime=2018-09-10/city=hangzhou
OK

hive> select * from partition_table where partition_table.daytime >= '2018-10-02';

# top N
hive> set mapred.reduce.tasks=1;
hive> select * from partition_table sort by rectime desc limit 2;