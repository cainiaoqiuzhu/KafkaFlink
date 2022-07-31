package com.ouxiaojie.kafkaflink.sink;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.ouxiaojie.kafkaflink.entity.Student;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class KafkaToMysql extends RichSinkFunction<Student> {

    private DataSource dataSource;

    private static final String BASE_SQL = "INSERT INTO student_table " +
            "(`stu_id`, " +
            "`stu_name`, " +
            "`sex`, " +
            "`birth_date`, " +
            "`class_id`, " +
            "`partition1`," +
            "`offset`," +
            "`timestamp`" +
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE " +
            "`partition1`=values(`partition1`)," +
            "`offset`=values(`offset`)," +
            "`timestamp`=values(`timestamp`)";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Map<String, String> parameterMap = parameterTool.toMap();
        Properties properties = new Properties();
        properties.put(DruidDataSourceFactory.PROP_DRIVERCLASSNAME, parameterMap.get("mysql.driverClassName"));
        properties.put(DruidDataSourceFactory.PROP_URL, parameterMap.get("mysql.url"));
        properties.put(DruidDataSourceFactory.PROP_USERNAME, parameterMap.get("mysql.username"));
        properties.put(DruidDataSourceFactory.PROP_PASSWORD, parameterMap.get("mysql.password"));
        properties.put(DruidDataSourceFactory.PROP_INITIALSIZE, parameterMap.get("mysql.initialSize"));
        properties.put(DruidDataSourceFactory.PROP_MAXACTIVE, parameterMap.get("mysql.maxActive"));
        properties.put(DruidDataSourceFactory.PROP_MINIDLE, parameterMap.get("mysql.minIdle"));
        properties.put(DruidDataSourceFactory.PROP_MAXWAIT, parameterMap.get("mysql.maxWait"));
        properties.put(DruidDataSourceFactory.PROP_TIMEBETWEENEVICTIONRUNSMILLIS, parameterMap.get("mysql.timeBetweenEvictionRunsMillis"));
        properties.put(DruidDataSourceFactory.PROP_TIMEBETWEENEVICTIONRUNSMILLIS, parameterMap.get("mysql.timeBetweenEvictionRunsMillis"));
        properties.put(DruidDataSourceFactory.PROP_MINEVICTABLEIDLETIMEMILLIS, parameterMap.get("mysql.minEvictableIdleTimeMillis"));
        properties.put(DruidDataSourceFactory.PROP_POOLPREPAREDSTATEMENTS, parameterMap.get("mysql.poolPreparedStatements"));
        properties.put(DruidDataSourceFactory.PROP_TESTWHILEIDLE, parameterMap.get("mysql.testWhileIdle"));
        properties.put(DruidDataSourceFactory.PROP_TESTONBORROW, parameterMap.get("mysql.testOnBorrow"));
        properties.put(DruidDataSourceFactory.PROP_TESTONRETURN, parameterMap.get("mysql.testOnReturn"));
        dataSource = DruidDataSourceFactory.createDataSource(properties);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(Student value, Context context){
        /*主要是获取连接并插入数据库*/
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(BASE_SQL);
            preparedStatement.setString(1, value.getStuId());
            preparedStatement.setString(2, value.getStuName());
            preparedStatement.setObject(3, value.getSex());
            preparedStatement.setObject(4, value.getBirthDate());
            preparedStatement.setObject(5, value.getClassId());
            preparedStatement.setObject(6, value.getPartition1());
            preparedStatement.setObject(7, value.getOffset());
            preparedStatement.setObject(8, value.getTimestamp());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            System.out.println("执行SQL失败:"+e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null){
                    connection.close();
                }
            } catch (SQLException e) {
                System.out.println("error:"+e);
                throw new RuntimeException(e);
            }
        }
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}