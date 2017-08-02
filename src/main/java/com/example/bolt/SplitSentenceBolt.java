package com.example.bolt;

import com.sun.org.apache.xml.internal.serializer.OutputPropertiesFactory;
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
public class SplitSentenceBolt extends BaseRichBolt {
    private OutputCollector _outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String sentence = tuple.getString(0);

        // provide the delimiters for splitting the tweet
        String delims = "[ .,?!]+";

        // now split the tweet into tokens
        String[] tokens = sentence.split(delims);

        // for each token/word, emit it
        for (String token: tokens) {
            _outputCollector.emit(new Values(token));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence-word"));
    }
}
