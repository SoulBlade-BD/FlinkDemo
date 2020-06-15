package JoinUtils


import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.FileUtils

object DistributesCacheDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.registerCachedFile("d:\\data\\file\\a.txt", "b.txt")
    val data = env.fromElements("a", "b", "c", "d")
    val result = data.map(new RichMapFunction[String, String] {
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val myFile = getRuntimeContext.getDistributedCache.getFile("b.txt")
        val lines = FileUtils.readFileUtf8(myFile)
        val it = lines.iterator
        while (it.hasNext) {
          val line = it.next()
          println(line)
        }
      }

      override def map(value: String) = {
        value
      }
    })
    result.print()
  }
}
