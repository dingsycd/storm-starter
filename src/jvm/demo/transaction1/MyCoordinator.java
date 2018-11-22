package demo.transaction1;

import java.math.BigInteger;

import backtype.storm.transactional.ITransactionalSpout.Coordinator;
import backtype.storm.utils.Utils;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月22日</p>
 * @author dingd
 *
 */
public class MyCoordinator implements Coordinator<MyMeta>{

	public static int batch_num = 10;
	
	@SuppressWarnings("unused")
	@Override
	public MyMeta initializeTransaction(BigInteger txid, MyMeta prevMetadata) {
		long beginPoint = 0;
		if (prevMetadata == null) {
			beginPoint = 0;
		} else {
			beginPoint = prevMetadata.getBeginPoint() + prevMetadata.getNum();
		}
		MyMeta mata = new MyMeta();
		mata.setBeginPoint(beginPoint);
		mata.setNum(batch_num);
		System.out.println("启动一个事务" + mata.toString());
		return mata;
	}

	@Override
	public boolean isReady() {
		Utils.sleep(2000);
		return true;
	}

	@Override
	public void close() {
		
	}

}
