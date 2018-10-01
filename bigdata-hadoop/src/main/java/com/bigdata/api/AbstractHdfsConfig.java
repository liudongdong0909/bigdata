package com.bigdata.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 *
 * @author donggua
 * @version 1.0
 * @create 2018/10/1
 */
public abstract  class AbstractHdfsConfig {

    public static Configuration getConfiguration() {
        /**
         * 客户端去操作hdfs时，是有一个用户身份的
         * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
         * <p>
         * 也可以在构造客户端fs对象时，通过参数传递进去
         */
        Configuration configuration = new Configuration();
        // configuration.set("fs.defaultFS", "hdfs://hadoop01:9000");
        // configuration.set("dfs.replication", "5");
        return configuration;
    }

    public static FileSystem getFileSystem() {

        FileSystem fileSystem = null;
        try {
            Configuration configuration = getConfiguration();
            // fileSystem = FileSystem.get(configuration);

            //org.apache.hadoop.security.AccessControlException:
            // Permission denied: user=donggua, access=READ_EXECUTE, inode="/tmp":hadoop:supergroup:drwx------
            fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "hadoop");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fileSystem;
    }
}
