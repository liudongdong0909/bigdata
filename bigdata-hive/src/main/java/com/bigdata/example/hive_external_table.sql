hive>
    > create external table if not exists external_table (key string)
      row format delimited fields terminated by '\t' location '/hive_data';