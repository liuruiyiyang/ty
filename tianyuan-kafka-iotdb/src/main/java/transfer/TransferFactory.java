package transfer;

import dao.IKmxDAO;
import dao.KmxDaoFactory;
import transfer.imp.DataTypes;
import transfer.imp.Transfer;
import org.apache.log4j.Logger;

import com.sagittarius.bean.common.ValueType;

public class TransferFactory {

	private static IKmxDAO dao = KmxDaoFactory.getKMXDao();

	private static Logger logger = Logger.getLogger(TransferFactory.class);

	public static ITransfer getTransfer(){
		return new Transfer();
	}

	public static ITransfer getTransfer(String deviceId, String workStatusId){
		ValueType type = dao.getType(deviceId, workStatusId);
//		logger.debug("type: " + type);
		if(type != null)
			return DataTypes.getTranFunc(type);
		logger.debug(String.format("[找不到数据类型]%s的%s工况的数据类型为空", deviceId, workStatusId));
		return null;
	}
}
