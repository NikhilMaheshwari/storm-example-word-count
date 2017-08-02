package com.example;

import com.example.bolt.ExclamationBolt;
import com.example.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by nikzz on 02/08/17.
 */
public class SentenceExclamationTopology {
    public static void main(String[] args) {
        System.out.print("Hello World !!");

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("sentence-spout", new RandomSentenceSpout(),5);
        topologyBuilder.setBolt("exclamation-bolt1", new ExclamationBolt(), 3).shuffleGrouping("sentence-spout");
        topologyBuilder.setBolt("exclamation-bolt2", new ExclamationBolt(), 2).shuffleGrouping("sentence-spout");

        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        // create the local cluster instance
        LocalCluster cluster = new LocalCluster();

        // submit the topology to the local cluster
        cluster.submitTopology("sentence-exclamation", conf, topologyBuilder.createTopology());

        //**********************************************************************
        // let the topology run for 30 seconds. note topologies never terminate!
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            cluster.shutdown();
        }
        //**********************************************************************

        // we are done, so shutdown the local cluster
        cluster.shutdown();
    }
}
