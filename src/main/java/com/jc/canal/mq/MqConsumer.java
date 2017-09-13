package com.jc.canal.mq;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MqConsumer {
	public static void main(String[] arg) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "127.0.0.1:9092");
		props.put("group.id", "canal-binlog");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("auto.offset.reset", "earliest");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(Arrays.asList("172.18.6.7_3306_test_name","canal_binlog","remotetopic","10.0.31.145_3306_test_dt_audit", "10.0.31.145_3306_test_dt_canal_mq","10.0.31.145_3306_dtbus_dt_field"));
		try {
			  while (true) {
				  try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    ConsumerRecords<String, String> records = consumer.poll(1000);
//			    for (ConsumerRecord<String, String> record : records)
//			      System.out.println(record.topic()+"："+record.offset() + ": " + record.value().length());
			    System.out.println("收到消息数目"+records.count());
			  }
			} finally {
			  consumer.close();
			}
	}

}
