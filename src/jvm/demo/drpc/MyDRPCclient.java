package demo.drpc;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月20日</p>
 * @author dingd
 *
 */
public class MyDRPCclient {

	public static void main(String[] args) {
		DRPCClient client = new DRPCClient("192.168.1.107", 3772);
		try {
			String result = client.execute("exclamation", "demo");
			System.out.println(result);
		} catch (TException e) {
			e.printStackTrace();
		} catch (DRPCExecutionException e) {
			e.printStackTrace();
		}
	}

}
