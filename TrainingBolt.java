package com.keedio.storm.training;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import static backtype.storm.utils.Utils.tuple;

public class TrainingBolt extends BaseRichBolt {

	OutputCollector collector;
	public static final Logger LOG = Logger.getLogger(TrainingBolt.class);
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
		String customProperty = (String) stormConf.get("training.bolt.property");
		if (customProperty == null)
			customProperty = "";
	}

	@Override
	public void execute(Tuple input) {
		
		String inputMessage = new String(input.getBinary(0));
		
		String outputMessage = inputMessage + " -- Training Bolt Custom Porperty: { " +  " }";
		
		collector.emit(tuple(outputMessage));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("outputTuple"));
	}

}
