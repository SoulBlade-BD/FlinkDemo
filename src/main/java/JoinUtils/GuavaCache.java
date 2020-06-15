package JoinUtils;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.concurrent.TimeUnit;


public class GuavaCache {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, String>> textStream = env.socketTextStream("localhost", 9000, "\n")
                .map(p -> {
                    //输入格式为：userID,cityID
                    String[] message = p.split(",");
                    return new Tuple2<>(message[0], message[1]);
                })
                .returns(new TypeHint<Tuple2<String, String>>() {
                });
        DataStream<Tuple2<String, String>> result = textStream.map(new MapJoin());
        result.print();
        env.execute();
    }

    private static class MapJoin extends RichMapFunction<Tuple2<String, String>, Tuple2<String, String>> {
        LoadingCache<String, String> dim;

        @Override
        public void open(Configuration parameters) throws Exception {
            //使用google LoadingCache来进行缓存
            dim = CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(5, TimeUnit.MINUTES)
                    .refreshAfterWrite(1, TimeUnit.MINUTES)
                    .build(
                            new CacheLoader<String, String>() {
                                @Override
                                public String load(String cityID) throws Exception {
                                    String cityName = readFromHbase(cityID);
                                    return cityName;
                                }
                            }
                    );
        }

        @Override
        public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {
            String cityName = null;
            if(dim.get(value.f1)!=null){
                cityName = dim.get(value.f1);
            }
            //userID,cityName
            return new Tuple2<>(value.f0,cityName);
        }
    }

    private static String readFromHbase(String cityID) throws Exception {
        org.apache.hadoop.conf.Configuration cfg = HBaseConfiguration.create();
        cfg.addResource("/usr/local/hadoop/hadoop-client/conf/core-site.xml");
        cfg.addResource("/usr/local/hadoop/hadoop-client/conf/hdfs-site.xml");
        cfg.addResource("/usr/local/hbase/hbase-client/conf/hbase-site.xml");

        Connection conn = ConnectionFactory.createConnection(cfg);
        Table tbl = conn.getTable((TableName.valueOf("events_db:city")));

        String cityName = "no message";
        Get get = new Get(cityID.getBytes());

        if (!get.isCheckExistenceOnly()) {
            Result result = tbl.get(get);
            for (Cell cell : result.listCells()) {
                byte[] value = cell.getValueArray();
                cityName = new String(value);
                System.out.println(new String(value));
            }
        }
        return cityName;
    }
}
