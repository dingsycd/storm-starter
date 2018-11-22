package demo.transaction1;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月22日</p>
 * @author dingd
 *
 */
public class MyTxBolt extends BaseTransactionalBolt{
	private static final long serialVersionUID = 8090180852733775714L;
	Integer                   count            = 0;
	BatchOutputCollector      collector;
	TransactionAttempt        tx;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		this.collector = collector;
		System.err.println("MyTxBolt prepare" + id.getTransactionId() + " attemptid" + id.getAttemptId());
	}

	@Override
	public void execute(Tuple tuple) {
		tx = (TransactionAttempt) tuple.getValue(0);
		System.err.println("MyTxBolt TransactionAttempt" + tx.getTransactionId() + " attemptid" + tx.getAttemptId());
		String log = tuple.getString(1);
		if (log.length() > 0 && log != null) {
			count++;
		}
	}

	@Override
	public void finishBatch() {
		System.err.println("finishBatch" + count);
		collector.emit(new Values(tx, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx", "count"));
	}

}
