package edu.sjtu.se.dclab;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class HelloTopology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("RandomHelloSpout", new HelloSpout(), 10);
		builder.setBolt("HelloWorldBolt", new HelloBolt(), 2).shuffleGrouping(
				"RandomHelloSpout");

		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
//			LocalCluster cluster = new LocalCluster();
//			cluster.submitTopology("HelloTopology", conf,
//					builder.createTopology());
//			Utils.sleep(10000);
//			cluster.killTopology("HelloTopology");
//			cluster.shutdown();
		}
	}
}
