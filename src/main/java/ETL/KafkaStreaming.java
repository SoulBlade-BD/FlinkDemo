package ETL;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaStreaming {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("application.id", "ETL-application");
        props.put("bootstrap.servers", "test3:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());
//        props.put("state.dir", "/tmp/kafka-streams/events/0_0");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("user_friends");
        KStream<String, String> result = textLines.flatMap((k, v) -> transform(k, v)).filter((k, v) -> v != null && v.length() > 0);
        result.to("user_friends_etl", Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
    //value ----123,456 789 123
    static Iterable<KeyValue<String, String>> transform(String key, String value) {
        List<KeyValue<String, String>> items = new ArrayList();
        String[] fields = value.split(",", -1);
        if(fields.length==0){
            items.add(new KeyValue<>(key,""));
        }else{
            for(String vs :fields){
                items.add(new KeyValue(key, String.join(",", vs)));
            }
        }
        return items;
    }

    static List<String> transform(String[] fields) {
        List<String> results = new ArrayList();
        String user = fields[0];
        String[] friends = fields[1].split(" ", -1);
        if (friends != null && friends.length > 0) {
            for(String friend:friends){
                results.add(user+","+friend);
            }
        }
        return results;
    }
}
