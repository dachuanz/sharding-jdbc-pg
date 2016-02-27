package zdctest.zdc1;

import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.Conversion;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

public class RangeUtil {

	public static Integer partitioning(String id, int i) {

		byte[] bs = DigestUtils.sha1(id);

		UUID long1 = Conversion.byteArrayToUuid(bs, 0);

		double l = long1.getLeastSignificantBits() % 1024;

		int j = (new Double(Math.pow(2, i))).intValue();

		RangeMap<Double, Integer> rangeMap = TreeRangeMap.create();

		for (int k = 0; k < j; k++) {

			rangeMap.put(Range.closedOpen(k * Math.pow(2, 10 - i), (k + 1) * Math.pow(2, 10 - i)), k);

		}

		return rangeMap.get(Math.abs(l));
	}

}
