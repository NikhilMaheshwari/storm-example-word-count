package com.example.spout;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by nikzz on 30/07/17.
 */
public class WordSpout extends BaseRichSpout {

    private Random rnd;

    private SpoutOutputCollector collector;

    private String[] wordList;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        rnd = new Random(31);
        collector = spoutOutputCollector;
        wordList = new String[]{
                "Nikhil",
                "Akash",
                "Chirag",
                "Mohit"
        };
    }

    @Override
    public void nextTuple() {

        Utils.sleep(1000);

        // generate a random number based on the wordList length
        int nextInt = rnd.nextInt(wordList.length);

        // emit the word chosen by the random number from wordList
        collector.emit(new Values(wordList[nextInt]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
