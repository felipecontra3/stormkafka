package com.vishnu.storm.spout;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;

import java.util.Properties;

import com.vishnu.storm.Keys;

import org.apache.storm.spout.RawScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class SpoutBuilder {
	
	public Properties configs = null;
	
	public SpoutBuilder(Properties configs) {
		this.configs = configs;
	}
	public KafkaSpout buildKafkaSpout() {
		BrokerHosts hosts = new ZkHosts(configs.getProperty(Keys.KAFKA_ZOOKEEPER));
		String topic = configs.getProperty(Keys.KAFKA_TOPIC);
		String zkRoot = configs.getProperty(Keys.KAFKA_ZKROOT);
		String groupId = configs.getProperty(Keys.KAFKA_CONSUMERGROUP);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		return kafkaSpout;
	}
}
