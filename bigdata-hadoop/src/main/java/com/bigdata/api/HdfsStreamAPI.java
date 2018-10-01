package com.bigdata.api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * 用流的方式来操作hdfs上的文件
 * 可以实现读取指定偏移量范围的数据
 *
 * @author donggua
 * @version 1.0
 * @create 2018/10/1
 */
public class HdfsStreamAPI extends AbstractHdfsConfig {

    public static void main(String[] args) throws Exception {
        // upload();
        // download();
        // randomAccess();
        cat();
    }

    public static void upload() throws Exception {
        FileSystem fileSystem = getFileSystem();
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/donggua/zookeeper-3.4.8.tar.gz"), true);
        FileInputStream fileInputStream = new FileInputStream("/Users/donggua/Downloads/bigdata-tools/zookeeper-3.4.8.tar.gz");
        IOUtils.copy(fileInputStream, outputStream);
    }

    public static void download() throws Exception {
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream inputStream = fileSystem.open(new Path("/test/donggua/zookeeper-3.4.8.tar.gz"));
        FileOutputStream fileOutputStream = new FileOutputStream("/Users/donggua/Downloads/bigdata-tools/zookeeper-3.4.8.tar.gz");
        IOUtils.copy(inputStream, fileOutputStream);
    }

    public static void randomAccess() throws Exception {
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream inputStream = fileSystem.open(new Path("/input/test.txt"));
        inputStream.seek(20);

        FileOutputStream outputStream = new FileOutputStream("/Users/donggua/Downloads/dd.txt");
        IOUtils.copy(inputStream, outputStream);
    }

    public static void cat() throws Exception {
        FileSystem fileSystem = getFileSystem();
        FSDataInputStream inputStream = fileSystem.open(new Path("/input/wenben.txt"));
        IOUtils.copy(inputStream, System.out);
    }
}
