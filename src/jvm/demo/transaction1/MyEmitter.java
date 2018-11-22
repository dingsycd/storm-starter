package demo.transaction1;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout.Emitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月22日</p>
 * @author dingd
 *
 */
public class MyEmitter implements Emitter<MyMeta>{

	Map<Long, String> dbMap = null;
	
	public MyEmitter(Map<Long, String> dbMap) {
		this.dbMap = dbMap;
	}
	
	@Override
	public void emitBatch(TransactionAttempt tx, MyMeta coordinatorMeta, BatchOutputCollector collector) {
		long beginPoint = coordinatorMeta.getBeginPoint();
		int num = coordinatorMeta.getNum();
		for (long i = beginPoint; i < beginPoint + num; i++) {
			if (dbMap.get(i) == null) {
				continue;
			}
			collector.emit(new Values(tx, dbMap.get(i)));
		}
	}

	@Override
	public void cleanupBefore(BigInteger txid) {
		
	}

	@Override
	public void close() {
		
	}

}
