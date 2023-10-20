package com.superred.flink.demo.utils;

import com.superred.flink.demo.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Desc: 往kafka中写数据,可以使用这个main函数进行测试
 * Created by QiaoKang on 2023-10-19
 * Blog:
 */
public class KafkaUtils {

    public static final String broker_list = "10.66.77.92:9092";
    public static final String topic = "student";  //kafka topic 需要和 flink 程序用同一个 topic

    public static void writeKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        // 向 kafka主题student 写入数据
        for (int i = 301; i <= 320; i++) {
            Student student = new Student(i, "lisi" + i, "password" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>
                    (topic, null, null, GsonUtil.toJson(student));

            producer.send(record);
            System.out.println("发送数据: " + GsonUtil.toJson(student));
        }
        producer.flush();
    }

    public static void main(String[] args) {
        writeKafka();
    }
}
