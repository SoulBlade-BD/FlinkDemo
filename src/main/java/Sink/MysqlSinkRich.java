package Sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSinkRich extends RichSinkFunction<Tuple2<String,String>> {
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    String userName = "root";
    String password = "123456";
    String driverName = "com.mysql.jdbc.Driver";
    String dburl = "jdbc:mysql://localhost:3306/user_friend";

    public void invoke(Tuple2<String,String> value)throws Exception{
        if(connection==null){
            Class.forName(driverName);
            connection = DriverManager.getConnection(dburl,userName,password);
            String sql  = "insert into table user_friend values(?,?)";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,value._1);
            preparedStatement.setString(2,value._2);
            preparedStatement.executeUpdate();
            if(preparedStatement!=null){
                preparedStatement.close();
            }
            if(connection!=null){
                connection.close();
            }
        }
    }


}
