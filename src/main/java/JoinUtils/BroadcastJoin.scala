package JoinUtils


import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.util.parsing.json.JSON



class BroadcastJoin {
  def main(args: Array[String]): Unit = {
    //create a pageStream(pageId,pageName),a inputStream(word,pageId,timestamp)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pageStream = env.socketTextStream("localhost", 9999)
    val inputStream = env.socketTextStream("localhost", 9998)
    //自定义广播状态描述符
    var broadcastStateDesc = new MapStateDescriptor("broadcast_state",
      //key
      classOf[String],
      //value
      classOf[String]
    )
    //生成broadcastStream
//    val broadcastStream = pageStream.broadcast(broadcastStateDesc)
//    inputStream.connect(broadcastStream)
//      .process(new BroadcastProcessFunction[String,(String,String),String] {
//        override def processElement(value:String,
//                                    ctx:BroadcastProcessFunction[String,
//                                    (String,String),String]#ReadOnlyContext,
//                                    out:Collector[String]):Unit = {
//          val state =ctx.getBroadcastState(broadcastStateDesc)
//          val jsonObject = JSON.paseObject(value)
//        }



      })

  }
}
