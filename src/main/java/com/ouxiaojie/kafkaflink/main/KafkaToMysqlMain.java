package com.ouxiaojie.kafkaflink.main;

import com.ouxiaojie.kafkaflink.entity.Student;
import com.ouxiaojie.kafkaflink.operator.MyFilterFunction;
import com.ouxiaojie.kafkaflink.operator.MyMapFunction;
import com.ouxiaojie.kafkaflink.schema.MyMysqlSchema;
import com.ouxiaojie.kafkaflink.sink.KafkaToMysql;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaToMysqlMain {

    public static void main(String[] args) throws Exception {
        System.out.println("Flink任务开始启动================");
        // 1、Flink配置(初始化flink流处理的运行环境)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(KafkaToMysqlMain.class.getResourceAsStream("/kafka-flink.properties"));
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);

        // 2、checkpoint配置
        // 每隔2000ms进行启动一个检查点
        env.enableCheckpointing(5000);
        // 设定两个checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长
        // 积压的checkpoint过多，占用大量资源从而影响性能(1000毫秒)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // 只有一个checkpoint可以执行(默认情况下)
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        /*
         * 传入的string类型的是json格式对的，将其变成Student类型 --> map算子
         * 传入的班级如果不在1-12之间，则舍去 --> filter算子
         * */
        DataStream<String> stream = env.addSource(getConsummer(parameterTool)).setParallelism(1);
        DataStream<Student> mapstream = stream.map(new MyMapFunction()).setParallelism(1);
        DataStream<Student> filterstream = mapstream.filter(new MyFilterFunction()).setParallelism(1);
        filterstream.addSink(new KafkaToMysql()).setParallelism(1);
        env.execute();
    }

    private static FlinkKafkaConsumer getConsummer(ParameterTool parameterTool){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokers(parameterTool)); // 指定kafka的broker地址
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, parameterTool.get("group.id")); // 指定消费者id
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer(parameterTool.get("topic"), new MyMysqlSchema(), properties);
        return consumer;
    }

    private static String getBrokers(ParameterTool parameterTool){
        return parameterTool.get("bootstrap.servers");
    }
}
