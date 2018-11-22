package demo.transaction1;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.tuple.Fields;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月22日</p>
 * @author dingd
 *
 */
public class MyTxSpout implements ITransactionalSpout<MyMeta>{
	private static final long serialVersionUID = 7610336497170692027L;
	private Random random = new Random();
	//数据源
	Map<Long, String> dbMap = null;
	
	public MyTxSpout() {
		dbMap = new HashMap<Long, String>();
		String[] hosts = {"www.taobao.com"};
		String[] session_id = {"sdfafsadfasfasdf", "dsfasfasfasdfs", "sdfafasffasfasdf", "dsfasfdasfasdffdas"};
		String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
				"2014-01-07 08:40:54", "2014-01-07 08:40:55"};
		for (long i = 0; i < 100; i++) {
			dbMap.put(i, hosts[0] + "\t" + session_id[random.nextInt(4)] + "\t" + time[random.nextInt(5)]);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx", "log"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Coordinator<MyMeta> getCoordinator(Map conf, TopologyContext context) {
		return new MyCoordinator();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Emitter<MyMeta> getEmitter(Map conf, TopologyContext context) {
		return new MyEmitter(dbMap);
	}

}
