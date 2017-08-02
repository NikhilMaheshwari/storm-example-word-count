package com.example.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by nikzz on 02/08/17.
 */
public class ExclamationBolt extends BaseRichBolt {
    OutputCollector _outputCollection;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollection = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String word = tuple.getString(0);

        // build the word with the exclamation marks appended
        StringBuilder exclamatedWord = new StringBuilder();
        exclamatedWord.append(word).append("!!!");

        // emit the word with exclamations
        _outputCollection.emit(tuple, new Values(exclamatedWord.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("exclamated-word"));
    }
}
