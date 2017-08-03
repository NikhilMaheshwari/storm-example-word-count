package com.example;

import com.example.bolt.CountBolt;
import com.example.bolt.ParseTweetBolt;
import com.example.bolt.ResultBolt;
import com.example.spout.TweetSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by nikzz on 03/08/17.
 */
public class TweetTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        TweetSpout tweetSpout = new TweetSpout(
                "beQ8XgcrFcxjR0GE545kKVl3K",
                "drO4SA09qBaPwdM0RDBfZxZDwW1x4r6CJ8sVPWf6CfJDGuNenN",
                "2198315929-LREQlD8im1ScFmtfxyTYHY0evPyhmp2cW5rEErT",
                "IclHTUtGz4yqAD0y7dDs9YR9HbuSF4MxIei9NQEz4tFsf"
        );

        topologyBuilder.setSpout("tweet-spout", tweetSpout, 1);

        topologyBuilder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");

        topologyBuilder.setBolt("rolling-count-bolt", new CountBolt(), 1).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));

        topologyBuilder.setBolt("report-bolt", new ResultBolt(), 1).globalGrouping("rolling-count-bolt");

        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tweet-word-count",config, topologyBuilder.createTopology());

        Utils.sleep(300000);

        // now kill the topology
        cluster.killTopology("tweet-word-count");

        // we are done, so shutdown the local cluster
        cluster.shutdown();
    }
}
