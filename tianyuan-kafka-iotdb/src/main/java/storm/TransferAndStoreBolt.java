package storm;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.sagittarius.exceptions.DataTypeMismatchException;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.exceptions.UnregisteredHostMetricException;
import com.sagittarius.write.SagittariusWriter.Data;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import dao.Connection;
import dao.IKmxDAO;
import dao.KmxDaoFactory;
import transfer.ITransfer;
import transfer.TransferFactory;
import tsinghua.thss.sdk.write.Writer;
import ty.pub.BeanUtil;
import ty.pub.TransPacket;
import ty.pub.TransPacket.WorkStatusRecord;

public class TransferAndStoreBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 16546L;

	private OutputCollector _collector;

	private String raw;
	private TransPacket packet ;

	private String stampTimeID;
	
	IKmxDAO dao;
	Data data;
	
	
	
	SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

//	private Map<String, String> specialTypes;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		Connection.init(stormConf);
		raw = (String) stormConf.get("decodeData");
//		infoTypeKey = (String) stormConf.get("infoType_key");
		stampTimeID = (String) stormConf.get("stampTime_ID");
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
//		TransPacket packet = (TransPacket) input.getValue(0);
		
		//接收原始数据包
		try {
			packet  = KryoUtil.deserialize((byte[])input.getValue(0), TransPacket.class);
//			TransPacket packet = (TransPacket) input.getValue(0);
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("[转换失败]Byte转化为TransPacket失败");
		}

		//连接KMX
		try {
			dao = KmxDaoFactory.getKMXDao();
			data = dao.getBulkData();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("[转换失败]KMX连接失败");
		}
		
		//获取时间戳
		long time;
		try {
			time = getTimestamp(packet);
//			System.out.println("数据包的时间："+sdf.format(time));	
		} catch (Exception e) {
			System.out.println(BeanUtil.ObjectToString(packet));
			e.printStackTrace();
			return;
		}
		
		//----------------------存储数据-------------------------------
		//基础信息参数
		Iterator<?> iterator = packet.getBaseInfoMapIter();
		while (iterator.hasNext()) {
			@SuppressWarnings("unchecked")
			Map.Entry<String, String> entry = (Entry<String, String>) iterator.next();
			try {
				transferAndAdd(packet.getDeviceId(), entry.getKey(), time, packet.getTimestamp(), entry.getValue(), data,null);
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println("[数据存储]基础信息存储失败：" + entry.getKey());
			}
		}
		//工况参数
		iterator = packet.getWorkStatusMapIter();
		while (iterator.hasNext()) {
			WorkStatusRecord entry = (WorkStatusRecord) iterator.next();
			try {
				transferAndAdd(packet.getDeviceId(), entry.getWorkStatusId(), time + entry.getTime(), packet.getTimestamp(),entry.getValue(), data, null);
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println("[数据存储]工况信息存储失败："+entry.getWorkStatusId());
			}
		
		}
		
		//解析后的包（信息生成时间）
		try {
//			System.out.println(raw);
			data.addDatum(packet.getDeviceId(), raw, time, packet.getTimestamp(), JSON.toJSONString(packet));
		} catch (UnregisteredHostMetricException e) {
			// TODO Auto-generated catch block
			System.out.println("[数据存储]解析后数据包存储失败："+packet.getDeviceId());
			e.printStackTrace();
		} catch (DataTypeMismatchException e) {
			// TODO Auto-generated catch block
			System.out.println("[数据存储]解析后数据包存储失败："+packet.getDeviceId());
			e.printStackTrace();
		}
	
		
		//数据包存储
		boolean success = false;
		while (!success) {
			try {
				dao.bulkSave(data);
				success = true;
			} catch (NoHostAvailableException | QueryExecutionException | TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();				
				sleep(100);
				Connection.reInit();
			} catch (Exception e) {
				System.out.println("[未知异常]");
				e.printStackTrace();
				break;
			}
		}
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	private void transferAndAdd(String deviceId, String workStatusId, long realTime, long receiveTime, String value,
			Data data, Writer writer) {
		ITransfer transfer = TransferFactory.getTransfer(deviceId, workStatusId);
		if (transfer != null) {
			boolean success = false;
			int time = 0;
			while (!success) {
				try {
					time++;
					transfer.transferAndAdd(deviceId, workStatusId, realTime, receiveTime, value, data, writer);
					if (time <= 2) {
						success = true;
					}
				} catch (NoHostAvailableException | TimeoutException | QueryExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println("[转换失败]重连尝试第" + time + "次");
					sleep(100);
					Connection.reInit();
					//检查是否需要更新writer
					writer = writer == null?null:Connection.getWriter();
				} catch (Exception e) {
					System.out.println("[未知异常]");
					e.printStackTrace();
					break;
				}
			}
		}

	}

	private long getTimestamp(TransPacket packet) {
		return Long.parseLong(packet.getBaseInfoMap().get(stampTimeID));
	}
	
	private void sleep(int t){
		try {
			Thread.sleep(t);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}
