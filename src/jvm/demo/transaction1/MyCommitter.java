package demo.transaction1;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月22日</p>
 * @author dingd
 *
 */
public class MyCommitter extends BaseTransactionalBolt implements ICommitter{
	private static final long          serialVersionUID = 6908868222302695325L;
	public static final String         GLOBAL_KEY       = "GLOBAL_KEY";
	public static Map<String, DbValue> dbMap            = new HashMap<String, MyCommitter.DbValue>();
	int                                sum              = 0;
	TransactionAttempt                 id;
	BatchOutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		this.collector = collector;
		this.id = id;
	}

	@Override
	public void execute(Tuple tuple) {
		sum += tuple.getInteger(1);
	}

	@SuppressWarnings("unused")
	@Override
	public void finishBatch() {
		DbValue value = dbMap.get(GLOBAL_KEY);
		DbValue newValue;
		if (value == null || !value.txid.equals(id.getTransactionId())) {
			//更新数据库
			newValue = new DbValue();
			newValue.txid = id.getTransactionId();
			if (value == null) {
				newValue.count = sum;
			} else {
				newValue.count = value.count + sum;
			}
			dbMap.put(GLOBAL_KEY, newValue);
		} else {
			newValue = value;
		}
		System.out.println("total===================" + dbMap.get(GLOBAL_KEY).count);
//		collector.emit(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public static class DbValue
	{
		BigInteger txid;
		int count = 0;
	}
}
