package Sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaToConsole {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.server","localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());
        props.put("linger.ms",5000);
        props.put("enable.auto.commit","true");
        props.put("auto.offset.reset","earliest");

        KafkaConsumer consumer = new KafkaConsumer(props);
        List<TopicPartition> topics = Arrays.asList(new TopicPartition("user_friends_raw",0));
        consumer.assign(topics);
        consumer.seek(topics.get(0),0L);

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record:records){
                System.out.printf("offset=%d,key=%s,value=%s%n",record.offset(),record.key(),record.value());
            }
        }

    }
}
