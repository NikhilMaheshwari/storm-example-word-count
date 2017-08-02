package com.example;

import com.example.bolt.CountBolt;
import com.example.bolt.ExclamationBolt;
import com.example.bolt.ResultBolt;
import com.example.spout.WordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by nikzz on 02/08/17.
 */
public class ExclamationTopology  {
    public static void main(String[] args) {
        System.out.print("Hello World !!");

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("word-spout", new TestWordSpout(),5);
        topologyBuilder.setBolt("exclam-bolt1", new ExclamationBolt(), 3).shuffleGrouping("word-spout");
        topologyBuilder.setBolt("exclam-bolt2", new ExclamationBolt(), 2).shuffleGrouping("word-spout");

        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        // create the local cluster instance
        LocalCluster cluster = new LocalCluster();

        // submit the topology to the local cluster
        cluster.submitTopology("exclamqtion", conf, topologyBuilder.createTopology());

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
