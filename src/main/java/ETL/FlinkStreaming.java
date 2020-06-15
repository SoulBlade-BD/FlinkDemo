package ETL;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkStreaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000L));
        env.enableCheckpointing(5000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "ETL-application");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer("user_friends_raw", new SimpleStringSchema(), properties));
        DataStream<String> result = stream.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] items = s.split(",", -1);
                String[] friends = items[1].split(" ", -1);
                for (String friend : friends) {
                    collector.collect(items[0] + "," + friend);
                }
            }
        });

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer("test3:9092", "user_friends_etl", new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);
        result.addSink(myProducer);
        env.execute("FlinkETL");
    }
}
