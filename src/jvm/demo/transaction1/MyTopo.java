package demo.transaction1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.transactional.TransactionalTopologyBuilder;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月22日</p>
 * @author dingd
 *
 */
@SuppressWarnings("deprecation")
public class MyTopo {

	public static void main(String[] args) {
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("ttbId", "spoutId", new MyTxSpout(), 1);
		builder.setBolt("bolt1", new MyTxBolt(), 3).shuffleGrouping("spoutId");
		builder.setBolt("committer", new MyCommitter(), 1).shuffleGrouping("bolt1");
		Config conf = new Config();
		conf.setDebug(true);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.buildTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.buildTopology());
		}
	}

}
