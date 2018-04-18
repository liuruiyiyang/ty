package dao.imp;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.exceptions.TransportException;
import com.sagittarius.bean.common.ValueType;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.write.SagittariusWriter.Data;

import dao.Connection;
import dao.IKmxDAO;
import tsinghua.thss.sdk.bean.common.Device;
import tsinghua.thss.sdk.bean.common.DeviceType;
import tsinghua.thss.sdk.bean.common.Sensor;
import tsinghua.thss.sdk.read.Reader;

public class KmxDao implements IKmxDAO {

	long timestamp;
	
	public KmxDao() {
		boolean notEnd = true;
		while(notEnd){
			try {
				init();
				notEnd = false;
			} catch (NoHostAvailableException | TimeoutException | QueryExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}

	}

	Map<String, Map<String, ValueType>> deviceTypesAndSensor;

	//device -> devicetype
	Map<String, String> deviceTypeAndDevices;
	
	Set<String> missing;

	public Data getBulkData() {
		// TODO Auto-generated method stub
		return Connection.getWriter().newData();
	}

	public void bulkSave(Data data)
			throws NoHostAvailableException, QueryExecutionException, TimeoutException, TransportException {
		// TODO Auto-generated method stub
		Connection.getWriter().bulkInsert(data);

	}

	public ValueType getType(String deviceId, String sensorId) {
		ValueType res = null;
		if (deviceTypeAndDevices.containsKey(deviceId)) {
			if (deviceTypesAndSensor.containsKey(deviceTypeAndDevices.get(deviceId))) {
				return deviceTypesAndSensor.get(deviceTypeAndDevices.get(deviceId)).get(sensorId);
			}
		}
		if(missing.contains(deviceId + sensorId))
			return null;
		// TODO Auto-generated method stub
		try {
			res = Connection.getReader().getValueType(deviceId, sensorId);
		} catch (NoHostAvailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (res != null && new Date().getTime() - timestamp > SYNC_INTERVAL) {
			try {
				init();
				System.out.println("[KMX]重载类型数据");
			} catch (NoHostAvailableException | TimeoutException | QueryExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else{
			missing.add(deviceId + sensorId); 
		}
		return res;
	}

	private synchronized void init() throws NoHostAvailableException, TimeoutException, QueryExecutionException {
		deviceTypesAndSensor = new HashMap<String, Map<String, ValueType>>();
		deviceTypeAndDevices = new HashMap<String, String>();
		Reader reader = Connection.getReader();
		List<DeviceType> types = null;
		List<Device> devices = null;
		types = reader.getAllDeviceType();
		for (DeviceType deviceType : types) {
			Map<String, ValueType> tmp = new HashMap<String, ValueType>();
			for (Sensor s : deviceType.getSensors()) {
				tmp.put(s.getId(), s.getValueType());
			}
			
			deviceTypesAndSensor.put(deviceType.getId(), tmp);
			devices = reader.getDevicesByType(deviceType.getId());
			
			if (devices != null)
				for (Device d : devices) {
					deviceTypeAndDevices.put(d.getId(), deviceType.getId());
				}
		}
		timestamp = new Date().getTime();
		missing = new HashSet<String>();
	}

}
