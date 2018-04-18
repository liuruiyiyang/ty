package transfer.imp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.write.SagittariusWriter.Data;

import transfer.ITransfer;
import tsinghua.thss.sdk.write.Writer;
import util.WorkStatus;
import util.WorkStatusRecorder;

public class LongTransfer extends ITransfer {

	public void transferAndAdd(String deviceId, String workStatusId, long timestamp, long timestamp2, String value,
			Data data, Writer writer) throws NoHostAvailableException, TimeoutException, QueryExecutionException {
		// TODO Auto-generated method stub
		long v = 0;
		try {
			v = Long.parseLong(value.split("\\.")[0]);
		} catch (NumberFormatException e) {
			try {
				v = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse(value).getTime();
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.out.println("[Long转换Error] （设备号： " + deviceId + ",工况号：" + workStatusId + ")");
				return;
			}
		}
		if (writer != null) {
			writer.insert(deviceId, workStatusId, timestamp, timestamp2, TimePartition.DAY, v);
			return;
		}
		if (data != null)
			data.addDatum(deviceId, workStatusId, timestamp, timestamp2, TimePartition.DAY, v);
//		if (saveFlag)
//			WorkStatusRecorder.getWorkStatusRecorder()
//					.recordWorkStatus(new WorkStatus(deviceId, workStatusId, timestamp, "long", v + ""));
	}

}
