package Sink;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink implements SinkFunction<Tuple2<String, String>> {
    private static final long serialVersionUID = 1L;
    String userName = "root";
    String password = "123456";
    String driverName = "com.mysql.jdbc.Driver";
    String dburl = "jdbc:mysql://localhost:3306/user_friend";

    @Override
    public void invoke(Tuple2<String, String> value) throws Exception {
        Class.forName(driverName);
        Connection connection = DriverManager.getConnection(dburl, userName, password);
        String sql = "insert into table user_friend values (?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value.f0);
        preparedStatement.setString(2, value.f1);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

}
