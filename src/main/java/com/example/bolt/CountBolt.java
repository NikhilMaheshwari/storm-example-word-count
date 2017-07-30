package com.example.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by nikzz on 30/07/17.
 */
public class CountBolt extends BaseRichBolt {

    private OutputCollector collector;

    // Map to store the count of the words
    private Map<String, Integer> countMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        countMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        if(countMap.get(word) == null){
            countMap.put(word, 1);
        } else {
            int count = countMap.get(word);

            countMap.put(word, ++count);
        }

        collector.emit(new Values(word, countMap.get(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
