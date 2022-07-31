package com.ouxiaojie.kafkaflink.schema;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class MyMysqlSchema implements KafkaDeserializationSchema<String> {
    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord){
        String value = new String(consumerRecord.value(), StandardCharsets.UTF_8);
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();
        long timestamp = consumerRecord.timestamp();
        JSONObject jsonObject = JSONObject.parseObject(value);
        jsonObject.put("offset", offset);
        jsonObject.put("partition1", partition);
        jsonObject.put("timestamp", timestamp);
        return jsonObject.toString();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
