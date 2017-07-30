package com.example;


import com.example.bolt.CountBolt;
import com.example.bolt.ResultBolt;
import com.example.spout.WordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Main {

    public static void main(String[] args) {
        System.out.print("Hello World !!");

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("word-spout", new WordSpout(),5);
        topologyBuilder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("word-spout", new Fields("word"));
        topologyBuilder.setBolt("report-bolt", new ResultBolt(), 1).globalGrouping("count-bolt");

        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        // create the local cluster instance
        LocalCluster cluster = new LocalCluster();

        // submit the topology to the local cluster
        cluster.submitTopology("word-count", conf, topologyBuilder.createTopology());

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
