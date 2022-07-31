package com.ouxiaojie.kafkaflink.entity;

import lombok.Data;

@Data
public class KafkaBase {
    private Integer partition1;
    private Long offset;
    private Long timestamp;
}
