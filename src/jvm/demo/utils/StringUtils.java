package demo.utils;
/**
 * <p>Description:  </p>
 * <p>Date: 2018年11月27日</p>
 * @author dingd
 *
 */
public class StringUtils {

	/**
	 * String转为Double[]
	 * @param s
	 * @return
	 */
	public static Double[] stringToDoubles(String s) {
		String subString = s.substring(1, s.length() - 1);
		String[] strings = subString.split(",");
		Double[] results = new Double[strings.length];
		for (int i = 0; i < results.length; i++) {
			results[i] = Double.valueOf(strings[i]);
		}
		return results;
	}
}
