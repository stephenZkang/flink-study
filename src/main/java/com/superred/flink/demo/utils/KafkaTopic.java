package com.superred.flink.demo.utils;

import lombok.Data;

/**
 * Desc: Kafka相关主题
 * Created By QiaoKang on 2021/1/20 14:28
 * Version 1.0
 */
public enum KafkaTopic {

    // 学生
    STUDENT_TOPIC("student"),
    // 测试
    KafkaTopic("test1");

    private String topic;

    KafkaTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }
}
