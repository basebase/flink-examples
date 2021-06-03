package com.moyu.flink.examples.sink;

import com.moyu.flink.examples.model.Student;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/***
 *      数据写入mysql测试
 */
public class MysqlSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketStream =
                env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Student> studentStream = socketStream
                .filter(value -> value != null && value.length() > 0)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new Student(Integer.parseInt(fields[0]), fields[1], Double.parseDouble(fields[2]));
                }).returns(Types.POJO(Student.class));

        studentStream.addSink(new StudentMySQLSink());

        env.execute("MySQL Sink Test Job");
    }


    private static class StudentMySQLSink extends RichSinkFunction<Student> {

        private Connection connection;
        private PreparedStatement insertStatement;
        private PreparedStatement updateStatement;

        /**
         * 在open方法中初始化连接
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = getConnection();
            insertStatement = connection.prepareStatement("insert into student(id, name, score) values(?,?,?)");
            updateStatement = connection.prepareStatement("update student set score = ? where id = ?");
        }

        @Override
        public void invoke(Student value, Context context) throws SQLException {

            updateStatement.setDouble(1, value.getScore());
            updateStatement.setInt(2, value.getId());
            updateStatement.execute();

            // 如果没更新成功, 表示mysql不存在数据, 进行插入
            if (updateStatement.getUpdateCount() == 0) {
                insertStatement.setInt(1, value.getId());
                insertStatement.setString(2, value.getName());
                insertStatement.setDouble(3, value.getScore());
                insertStatement.execute();
            }
        }

        @Override
        public void close() throws Exception {

            if (insertStatement != null)
                insertStatement.close();

            if (updateStatement != null)
                updateStatement.close();

            if (connection != null)
                connection.close();
        }

        private Connection getConnection() {
            try {
                String className = "com.mysql.cj.jdbc.Driver";
                Class.forName(className);
                String url = "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&autoReconnect=true";
                String user = "root";
                String pass = "123456";
                Connection connection = DriverManager.getConnection(url, user, pass);
                return connection;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return null;
        }
    }
}
