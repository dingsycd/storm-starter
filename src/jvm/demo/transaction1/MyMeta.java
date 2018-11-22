package demo.transaction1;

import java.io.Serializable;

/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月22日</p>
 * @author dingd
 *
 */
public class MyMeta implements Serializable {
	private static final long serialVersionUID = -7489565036616854636L;
	private long beginPoint; //事务开始位置
	private int num; //batch的tuple个数
	
	@Override
	public String toString() {
		return getBeginPoint() + "-----" + getNum();
	}

	/**
	 * 获得事务开始位置
	 * @return
	 */
	public long getBeginPoint() {
		return beginPoint;
	}
	
	/**
	 * 设置事务开始位置
	 * @param beginPoint
	 */
	public void setBeginPoint(long beginPoint) {
		this.beginPoint = beginPoint;
	}
	
	/**
	 * 获得batch的tuple个数
	 * @return
	 */
	public int getNum() {
		return num;
	}
	
	/**
	 * 设置batch的tuple个数
	 * @param num
	 */
	public void setNum(int num) {
		this.num = num;
	}

}
