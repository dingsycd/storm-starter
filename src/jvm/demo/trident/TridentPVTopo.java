package demo.trident;

import java.util.HashMap;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import demo.trident.functions.MySplit;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;


public class TridentPVTopo {
  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  @SuppressWarnings("unchecked")
public static StormTopology buildTopology(LocalDRPC drpc) {
	  Random random = new Random();
	  String[] hosts = {"www.taobao.com"};
	  String[] session_id = {"sdfafsadfasfasdf", "dsfasfasfasdfs", "sdfafasffasfasdf", "dsfasfdasfasdffdas"};
	  String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-05 08:40:52", "2014-01-07 08:40:53", 
			  "2014-01-05 08:40:54", "2014-01-05 08:40:55"};
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("eachLog"), 3, 
    		new Values(hosts[0] + "\t" + session_id[random.nextInt(4)] + "\t" + time[random.nextInt(6)]),
    		new Values(hosts[0] + "\t" + session_id[random.nextInt(4)] + "\t" + time[random.nextInt(6)]),
    		new Values(hosts[0] + "\t" + session_id[random.nextInt(4)] + "\t" + time[random.nextInt(6)]),
    		new Values(hosts[0] + "\t" + session_id[random.nextInt(4)] + "\t" + time[random.nextInt(6)]),
    		new Values(hosts[0] + "\t" + session_id[random.nextInt(4)] + "\t" + time[random.nextInt(6)]),
    		new Values(hosts[0] + "\t" + session_id[random.nextInt(4)] + "\t" + time[random.nextInt(6)]));
    spout.setCycle(false);

    TridentTopology topology = new TridentTopology();
    TridentState wordCounts = topology.newStream("spout1", spout)
    		//.parallelismHint(16)
    		.each(new Fields("eachLog"), new MySplit("\t"), new Fields("date", "session_id"))
    		.groupBy(new Fields("date"))
    		.persistentAggregate(new MemoryMapState.Factory(), new Fields("session_id"), new Count(), new Fields("PV"))
    		//.parallelismHint(16)
    		;

    topology.newDRPCStream("getPV", drpc)
    .each(new Fields("args"), new Split(), new Fields("date"))
    .groupBy(new Fields("date"))
    .stateQuery(wordCounts, new Fields("date"), new MapGet(), new Fields("PV"))
    .each(new Fields("PV"), new FilterNull());
    //.aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
      for (int i = 0; i < 100; i++) {
        System.out.println("DRPC RESULT: " + drpc.execute("getPV", "2014-01-07 2014-01-05"));
        Thread.sleep(1000);
      }
    }  
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
  }
}
