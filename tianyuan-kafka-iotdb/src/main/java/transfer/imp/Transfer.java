package transfer.imp;

import org.apache.log4j.Logger;

import com.sagittarius.bean.common.ValueType;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.write.SagittariusWriter.Data;
import tsinghua.thss.sdk.write.Writer;

import dao.IKmxDAO;
import dao.KmxDaoFactory;
import transfer.ITransfer;

public class Transfer extends ITransfer {

	private static Logger logger = Logger.getLogger(Transfer.class);
	
	IKmxDAO dao;
	
	public Transfer(){
		dao = KmxDaoFactory.getKMXDao();
	}
	
	public void transferAndAdd(String deviceId, String workStatusId, long timestamp, long timestamp2, String value, Data data, Writer writer)  throws NoHostAvailableException, TimeoutException, QueryExecutionException{
		// TODO Auto-generated method stub
		ValueType type = dao.getType(deviceId, workStatusId);
//		logger.debug("type: " + type);
		if(type != null)
			DataTypes.getTranFunc(type).transferAndAdd(deviceId, workStatusId, timestamp, timestamp2, value, data, writer);
		else
			logger.debug(String.format("%s的%s工况的数据类型为空", deviceId, workStatusId));
	}

}
