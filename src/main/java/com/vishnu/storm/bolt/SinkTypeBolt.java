package com.vishnu.storm.bolt;

import java.util.Map;

import com.vishnu.storm.Topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class SinkTypeBolt extends BaseRichBolt {


	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	

	public void execute(Tuple tuple) {
		String value = tuple.getString(0);
		System.out.println("Received in SinkType bolt : "+value);
		int index = value.indexOf(" ");
		if (index == -1)
			return;
		String type = value.substring(0,index);
		System.out.println("Type : "+type);
		value = value.substring(index);
		if (type.equals("hdfs")) {
			collector.emit(Topology.HDFS_STREAM,new Values(type,value));
			System.out.println("Emitted : "+value);
		} else if (type.equals("mongo")) {
			collector.emit(Topology.MONGODB_STREAM,new Values(type,value));
			System.out.println("Emitted : "+value);
		}
		collector.ack(tuple);	
	}


	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Topology.HDFS_STREAM, new Fields( "sinkType","content" ));
		declarer.declareStream(Topology.MONGODB_STREAM, new Fields( "sinkType","content" ));
	}

}
