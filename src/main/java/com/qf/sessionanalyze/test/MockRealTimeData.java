package com.qf.sessionanalyze.test;

import com.qf.sessionanalyze.constant.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class MockRealTimeData extends Thread {
	
	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};  
	private static final Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();
	
	private KafkaProducer<Integer, String> producer;
	
	public MockRealTimeData() {
		provinceCityMap.put("Jiangsu", new String[]{"Nanjing", "Suzhou"});
		provinceCityMap.put("Hubei", new String[]{"Wuhan", "Jingzhou"});
		provinceCityMap.put("Hunan", new String[]{"Changsha", "Xiangtan"});
		provinceCityMap.put("Henan", new String[]{"Zhengzhou", "Luoyang"});
		provinceCityMap.put("Hebei", new String[]{"Shijiazhuang", "Zhangjiakou"});
		createProducerConfig();
	}

	/*
	private ProducerConfig createProducerConfig() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.88.81:9092,192.168.88.82:9092,192.168.88.83:9092");
		return new ProducerConfig(props);
	}
	*/
	private KafkaProducer createProducerConfig() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.138.101:9092,192.168.138.102:9092,192.168.138.103:9092");
		props.put("bootstrap.servers","192.168.138.101:9092,192.168.138.102:9092,192.168.138.103:9092");
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		producer =  new KafkaProducer(props);
		return producer;
	}
	public void run() {
		while(true) {
			String province = provinces[random.nextInt(5)];  
			String city = provinceCityMap.get(province)[random.nextInt(2)];
			// 数据格式为：timestamp province city userId adId
			String log = new Date().getTime() + " " + province + " " + city + " " 
					+ random.nextInt(20) + " " + random.nextInt(5);
			producer.send(new ProducerRecord<Integer, String>(Constants.KAFKA_TOPICS,log));
//			producer.send(new KeyedMessage<Integer, String>("AdRealTimeLog", log));
			try {
				System.out.println(log);
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData MessageProducer = new MockRealTimeData();
		System.out.println("********");
		MessageProducer.start();
	}
	
}
