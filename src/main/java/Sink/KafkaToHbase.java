package Sink;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaToHbase {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.server", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());
        props.put("linger.ms", 5000);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
        consumer.subscribe(Arrays.asList(new String[]{"user_friend"}));

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(3000);
                write(records);
        }
    }
    public static int write(ConsumerRecords<String,String> records) throws Exception{
        Configuration cfg = HBaseConfiguration.create();
        cfg.addResource("/usr/local/hadoop/hadoop-client/conf/core-site.xml");
        cfg.addResource("/usr/local/hadoop/hadoop-client/conf/hdfs-site.xml");
        cfg.addResource("/usr/local/hbase/hbase-client/conf/hbase-site.xml");

        Connection conn = ConnectionFactory.createConnection(cfg);
        Table tbl = conn.getTable((TableName.valueOf("events_db:user_friend")));

        List<Put> puts = new ArrayList<Put>();
        for(ConsumerRecord<String,String>record:records){
            String[] elements = record.value().split(",",-1);
            Put p = new Put(Bytes.toBytes(elements[0]+elements[1]));
            p= p.addColumn(Bytes.toBytes("User"),Bytes.toBytes("UserID"),Bytes.toBytes(elements[0]));
            p= p.addColumn(Bytes.toBytes("Friend"),Bytes.toBytes("FriendID"),Bytes.toBytes(elements[1]));
            puts.add(p);
        }
        if(puts.size()>0){
            tbl.put(puts);
        }
        int numPuts = puts.size();
        tbl.close();
        conn.close();
        return numPuts;
    }

}
