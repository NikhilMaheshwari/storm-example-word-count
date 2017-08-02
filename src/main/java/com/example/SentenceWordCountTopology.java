package com.example;

import com.example.bolt.CountBolt;
import com.example.bolt.ResultBolt;
import com.example.bolt.SplitSentenceBolt;
import com.example.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by nikzz on 02/08/17.
 */
public class SentenceWordCountTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("sentence-spout", new RandomSentenceSpout(),1);
        topologyBuilder.setBolt("sentence-split", new SplitSentenceBolt(), 5).shuffleGrouping("sentence-spout");
        topologyBuilder.setBolt("count-bolt", new CountBolt(), 5).fieldsGrouping("sentence-split", new Fields("sentence-word"));
        topologyBuilder.setBolt("result-bolt", new ResultBolt(), 1).globalGrouping("count-bolt");

        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sentence-word-count", config, topologyBuilder.createTopology());

        try{
            Thread.sleep(30000);
        }
        catch (InterruptedException ex){
            cluster.shutdown();
        }

        cluster.shutdown();
    }
}
