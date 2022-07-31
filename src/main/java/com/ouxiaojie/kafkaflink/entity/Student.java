package com.ouxiaojie.kafkaflink.entity;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Student extends KafkaBase implements Serializable {

    private Long id;

    private String stuId;

    private String stuName;

    private Integer sex;

    private Date birthDate;

    private Integer classId;
}
