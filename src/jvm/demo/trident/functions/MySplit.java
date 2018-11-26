package demo.trident.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月26日</p>
 * @author dingd
 *
 */
public class MySplit extends BaseFunction {
	private static final long serialVersionUID = -2309206732763744180L;
	String patton = null;
	
	public MySplit(String patton) {
		this.patton = patton;
	}
	
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String log = tuple.getString(0);
      String logArr[] = log.split(patton);
      if (logArr.length == 3) {
    	  collector.emit(new Values(logArr[2], logArr[1]));
      } 
	}

}
