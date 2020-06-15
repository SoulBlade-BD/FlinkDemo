package JoinUtils

import java.sql.{Connection, DriverManager}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class DimFlatMapFunction extends RichFlatMapFunction[(Int, String), (Int, String, String)] {
  var dim: Map[Int, String] = Map()
  var connection: Connection = _
//需要周期性调度open函数，实现维度数据周期性更新
  override def open(conf: Configuration): Unit = {
    super.open(conf)
    Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://localhost:3306/demo"
    val username = "root"
    val password = "123456"
    connection = DriverManager.getConnection(url, username, password)
    val sql = "select pid,pname from dim_product;"
    val statement = connection.prepareStatement(sql)
    try {
      val resultSet = statement.executeQuery()
      while (resultSet.next()) {
        var pid = resultSet.getInt("pid")
        var pname = resultSet.getString("pname")
        dim += (pid -> pname)
      }
    } catch {
      case e: Exception => println(e.getMessage)
    }
    connection.close()
  }

  override def flatMap(in: (Int, String), out: Collector[(Int, String, String)]): Unit = {
    val probeID = in._1
    if (dim.contains(probeID)) {
      out.collect((in._1, in._2, dim.get(probeID).toString))
    }
  }
}
