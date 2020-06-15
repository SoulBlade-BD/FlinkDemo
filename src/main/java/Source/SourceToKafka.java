package Source;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import java.io.*;
import java.util.Properties;


public class SourceToKafka {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.server","localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());
        props.put("linger.ms",5000);

        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        InputStream inputStream = null;
        BufferedReader bufferedReader = null;
        String line = null;
        inputStream = new FileInputStream("/data/user_friends.csv");
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        line = bufferedReader.readLine();

        while (line!=null){
            line = bufferedReader.readLine();
            producer.send(new ProducerRecord<>("user_friends_raw",String.valueOf(line)));
        }
        bufferedReader.close();
        inputStream.close();
        producer.close();

    }
}
