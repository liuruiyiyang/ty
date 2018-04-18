package transfer;

import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.write.SagittariusWriter.Data;

import tsinghua.thss.sdk.write.Writer;

public abstract class ITransfer {
	
	public static boolean saveFlag;

	public abstract void transferAndAdd(String deviceId, String workStatusId, long timestamp, long timestamp2, String value, Data data, Writer writer)  throws NoHostAvailableException, TimeoutException, QueryExecutionException;
	
}
