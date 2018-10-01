package com.bigdata.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * $$ HDFS API
 *
 * @author donggua
 * @version 1.0
 * @create 2018/10/1
 */
public class HdfsAPI extends AbstractHdfsConfig{

    public static void main(String[] args) throws Exception {
        // readFile();
        // upload();
        // download();
        // getConfig();
        // mkDirs();
        //  delete();
        fileStatus();
    }

    public static void fileStatus() throws IOException {
        FileSystem fileSystem = getFileSystem();
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        Stream.of(fileStatuses)
                .forEach(fileStatus -> {
                    System.out.println("name: " + fileStatus.getPath().getName() + " --> " + (fileStatus.isFile() ? "F" : "D"));
                });
    }

    public static void delete() throws IOException {
        FileSystem fileSystem = getFileSystem();
        boolean delete = fileSystem.delete(new Path("/test"), true);
        System.out.println(delete);
    }

    public static void mkDirs() throws IOException {
        FileSystem fileSystem = getFileSystem();
        boolean mkdirs = fileSystem.mkdirs(new Path("/test/donggua"));
        System.out.println(mkdirs);
    }

    public static void getConfig() {
        Configuration configuration = getConfiguration();
        Iterator<Map.Entry<String, String>> iterator = configuration.iterator();
        Stream.of(iterator)
                .forEach(element -> {
                    Map.Entry<String, String> entry = element.next();
                    System.out.println(entry.getKey() + " ==" + entry.getValue());
                });

    }

    public static void download() throws IOException {
        FileSystem fileSystem = getFileSystem();
        fileSystem.copyToLocalFile(new Path("/zookeeper-3.4.8.tar.gz"), new Path("/Users/donggua/Downloads/bigdata-tools/zookeeper-3.4.8.tar.gz"));
    }

    public static void upload() throws IOException {
        FileSystem fileSystem = getFileSystem();
        fileSystem.copyFromLocalFile(new Path("/Users/donggua/Downloads/bigdata-tools/zookeeper-3.4.8.tar.gz"), new Path("/"));
    }


    public static void readFile() throws IOException {

        FileSystem fileSystem = getFileSystem();
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (fileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = fileStatusRemoteIterator.next();
            System.out.println("blockSize: " + locatedFileStatus.getBlockSize());
            System.out.println("owner：" + locatedFileStatus.getOwner());
            System.out.println("replication: " + locatedFileStatus.getReplication());
            System.out.println("permission: " + locatedFileStatus.getPermission());
            System.out.println("name: " + locatedFileStatus.getPath().getName());

            System.out.println("================================");

            for (BlockLocation blockLocation : locatedFileStatus.getBlockLocations()) {
                System.out.println("块起始偏移量：" + blockLocation.getOffset());
                System.out.println("块长度：" + blockLocation.getLength());
                // 块所在的datanode节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("datanode: " + host);
                }
            }
        }

    }

}
