package dao;

import com.sagittarius.bean.common.ValueType;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.write.SagittariusWriter.Data;

public interface IKmxDAO {
	
	static final int SYNC_INTERVAL = 180000;

	public Data getBulkData();
	
	public void bulkSave(Data data) throws NoHostAvailableException, QueryExecutionException, TimeoutException;
	
	public ValueType getType(String deviceId, String sensorId);
	
}
