hive> create table bucket_table(id string) clustered by (id) into 4 buckets;
OK
Time taken: 0.172 seconds


hive> sethive.enforce.bucketing=true;
hive> insert into table bucket_table select name from stu;
hive> insert overwrite table bucket_table select name from stu;