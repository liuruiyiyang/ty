package Kyro;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.alibaba.fastjson.JSONObject;
import dao.IoTDBDaoFactory;
import dao.imp.IoTDBDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ty.pub.BeanUtil;
import ty.pub.ParsedPacketDecoder;
import ty.pub.ParsedPacketEncoder;
import ty.pub.TransPacket;
import util.MetaData;

public class TransPacketTest {
	private static org.apache.kafka.clients.consumer.KafkaConsumer<String,  byte[]> consumer;
	private static IoTDBDao dao = IoTDBDaoFactory.getIoTDBDao();
	private static final String DEFAULT_PATH_PREFIX = "root";

	public static void main(String[] args) {
		JSONObject jsonObject = JSONObject.parseObject("{"
				+ "'bootstrap.servers':'192.168.10.31:9092,192.168.10.32:9092,192.168.10.33:9092',"
				+ "'group.id':'java-iotdb-stormtest',"
				+ "'topic':'TY_PP_KTP_CTY_Decode',"
				+ "'enable.auto.commit':'true',"
				+ "'auto.commit.interval.ms':'1000',"
				+ "'key.deserializer':'org.apache.kafka.common.serialization.StringDeserializer',"
				+ "'value.deserializer':'org.apache.kafka.common.serialization.ByteArrayDeserializer',"
				+ "'kmx_url':'192.168.15.114,192.168.15.115,192.168.15.119',"
				+ "'kmx_linger_ms':'2',"
				+ "'kmx_max_writer_num':'1',"
				+ "'writer_cache':'false',"
				+ "'number_of_consumer':'1',"
				+ "'number_of_tranfer':'1',"
				+ "'workers':'3',"
				+ "'monitor_flag':'false',"
				+ "'save_flag':'false',"
				+ "'stampTime_ID':'TY_0001_00_6',"
				+ "'infoType_key':'TY_0001_00_5',"
				+ "'infoType_value':'20,24,26,28,30,2A',"
				+ "'newStampTime_ID':'TY_0002_00_730,TY_0002_00_731,TY_0002_00_732,TY_0002_00_733,TY_0002_00_734,TY_0002_00_734',"
				+ "'decodeData':'TY_0001_Decode_Packet'"
				+ "}");
		Map config = new HashMap();

		for (Object map : jsonObject.entrySet()){
			config.put(((Map.Entry)map).getKey(), ((Map.Entry)map).getValue());
		}

		consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, byte[]>(config);
		consumer.subscribe(Arrays.asList((String)config.get("topic")));
		consumer.seekToBeginning(consumer.assignment());

		int count = 0;
		Statement statement = null;
		boolean flag = false;
		while (true) {
			ConsumerRecords<String, byte[]> records = consumer.poll(100);
			for (ConsumerRecord<String, byte[]> record : records) {
//				TransPacket tmp = KryoUtil.deserialize(record.value(), TransPacket.class);
				TransPacket packet = new ParsedPacketDecoder().deserialize("", record.value());

				//flag = IoTDBInsert(tmp, count, statement,flag);

				//获取时间戳
				long time;
				try {
					time = getTimestamp(packet);
				} catch (Exception e) {
					System.out.println(BeanUtil.ObjectToString(packet));
					e.printStackTrace();
					return ;
				}

				//存储数据
				Iterator<?> iterator = packet.getWorkStatusMapIter();
				String deviceNO = packet.getDeviceId();

				long errorNum = 0;
				if(!flag) {
					java.sql.Connection connection = dao.getConnection();
					try {
						statement = connection.createStatement();
					} catch (SQLException e) {
						System.out.println("[转换失败]IoTDB连接失败");
					}
					flag = true;
				}


				if(statement != null) {
					while (iterator.hasNext()) {
						TransPacket.WorkStatusRecord entry = (TransPacket.WorkStatusRecord) iterator.next();
						String sensorName = entry.getWorkStatusId();
						String sensorValue = entry.getValue();
						long timeStamp = time + entry.getTime();
						String deviceSeries = null;
						String type = null;

						type = MetaData.getInstance().getSensorType(sensorName);


						deviceSeries = MetaData.getInstance().getDeviceSeriesID(packet.getDeviceId());

						if(deviceSeries != null && type != null) {
							String sql ;
							if(type.equals("String")) {
								sql = createInsertSQLStatment("type_" + deviceSeries, "d_" + deviceNO, "s_" + sensorName, "\"" + sensorValue + "\"", timeStamp);
							} else {
								sql = createInsertSQLStatment("type_" + deviceSeries, "d_" + deviceNO, "s_" + sensorName, sensorValue, timeStamp);
							}

							try {
								statement.addBatch(sql);
								count ++;
								System.out.println("count:"+count);
							} catch (SQLException e) {
								System.out.println("[转换失败]IoTDB添加批写入语句失败");
								e.printStackTrace();
							}
						}
					}
					if(count > 100) {
						flag = false;
						try {
							System.out.println("[数据存储]IoTDB执行批写入，语句数：" + count);
							statement.executeBatch();
						} catch (BatchUpdateException e) {
							long[] arr = e.getLargeUpdateCounts();
							for (long i : arr) {
								if (i == -3) {
									errorNum++;
								}
							}
							System.out.println("[数据存储]IoTDB批写入执行失败，失败数：" + errorNum);
							e.printStackTrace();
						} catch (SQLException e) {
							e.printStackTrace();
						}
						try {
							statement.clearBatch();
						} catch (SQLException e) {
							System.out.println("[数据存储]IoTDB批写入语句清理失败");
							e.printStackTrace();
						}
						try {
							statement.close();
						} catch (SQLException e) {
							System.out.println("[数据存储]IoTDB连接关闭失败");
							e.printStackTrace();
						}
						count = 0;
					}


				}
			}
		}

	}


	private static String createInsertSQLStatment(String storageGroup, String device, String sensor, String value, long time) {

		StringBuilder builder = new StringBuilder();
		builder.append("insert into ").append(DEFAULT_PATH_PREFIX).append(".").append(storageGroup).append(".").append(device).append("(timestamp");
		builder.append(",").append(sensor);
		builder.append(") values(");
		builder.append(time);
		builder.append(",").append(value);
		builder.append(")");
		System.out.println("sql:   " + builder.toString());
		return builder.toString();
	}


	private static long getTimestamp(TransPacket packet) {
		return Long.parseLong(packet.getBaseInfoMap().get("TY_0001_00_6"));
	}

}
