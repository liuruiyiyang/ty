package transfer.imp;

import com.sagittarius.bean.common.TimePartition;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;

import com.sagittarius.write.SagittariusWriter.Data;

import transfer.ITransfer;
import tsinghua.thss.sdk.write.Writer;
import util.WorkStatus;
import util.WorkStatusRecorder;

public class StringTransfer extends ITransfer {

	public void transferAndAdd(String deviceId, String workStatusId, long timestamp, long timestamp2, String value,
			Data data, Writer writer) throws NoHostAvailableException, TimeoutException, QueryExecutionException {
		// TODO Auto-generated method stub
		if (writer != null) {
			writer.insert(deviceId, workStatusId, timestamp, timestamp2, TimePartition.DAY, value);
			return;
		}
		if (data != null)
			data.addDatum(deviceId, workStatusId, timestamp, timestamp2, TimePartition.DAY, value);
//		if (saveFlag)
//			WorkStatusRecorder.getWorkStatusRecorder()
//					.recordWorkStatus(new WorkStatus(deviceId, workStatusId, timestamp, "String", value));
	}

}
