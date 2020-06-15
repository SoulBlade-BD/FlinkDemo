package StateReuse;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.streams.state.RocksDBConfigSetter;

public class checkPoint {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint
        env.enableCheckpointing(180000L, CheckpointingMode.EXACTLY_ONCE);
        //job取消后，可以恢复
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置State存储格式及时长

        //设置检查点路径
        String checkPointDir = "hdfs://localhost:40010/history/Flink/checkpoint";
        //进行状态存储
        env.setStateBackend(new FsStateBackend(checkPointDir, true));
        //增量状态存储
        RocksDBConfigSetter rkdb ;

    }
}
