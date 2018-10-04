package com.bigdata.example.flow.partitioner;

import com.bigdata.example.flow.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * 按照省份分区
 * <p>
 * K2  V2  对应的是map输出kv的类型
 *
 * @author donggua
 * @version 1.0
 * @create 2018/10/2
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 此处相当于从 数据库或者redis中获取省份分区标记的相关信息，
     * 应该在初始化的时候就获取数据，避免的getPartition中重复获取数据，这样非常大量的请求会干掉数据库或redis。
     */
    public static HashMap<String, Integer> proviceDict = new HashMap<>(4);

    static {
        proviceDict.put("136", 0);
        proviceDict.put("137", 1);
        proviceDict.put("138", 2);
        proviceDict.put("139", 3);
    }

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        // 按照手机号前三位来区分身份
        String prefix = key.toString().substring(0, 3);
        Integer provinceId = proviceDict.get(prefix);
        return provinceId == null ? 4 : provinceId;
    }
}
