package run;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.alibaba.fastjson.JSONObject;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import conf.IoTDBConfig;
import dao.Connection;
import dao.IoTDBDaoFactory;
import dao.KmxDaoFactory;

import dao.imp.IoTDBDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;
import storm.ConsumerSpout;
import storm.IoTDBTransferAndStoreBolt;

import thread.ConsumerThreadV2;
import ty.pub.BeanUtil;
import ty.pub.ParsedPacketDecoder;
import ty.pub.TransPacket;
import util.MetaData;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Launcher {
	private static org.apache.kafka.clients.consumer.KafkaConsumer<String,  byte[]> consumer;
	private static IoTDBDao dao = IoTDBDaoFactory.getIoTDBDao();
	private static final String DEFAULT_PATH_PREFIX = "root";
	public  static final int consumerNum = 40;
	static Logger logger = Logger.getLogger(Launcher.class);

	public static void main(String[] args) throws IOException, AlreadyAliveException, InvalidTopologyException {
		if(args.length > 1) {
			IoTDBConfig.builder().setHost(args[0]);
		}
		//new Launcher().launch(args);
        Launcher.launchMulThreadsWithoutStormV2();
	}


    public static void launchMulThreadsWithoutStormV2(){
		System.out.print(new Date(System.currentTimeMillis()) + ";");
		System.out.println("[元数据获取]开始连接Oracle数据库获取元数据，约三分钟...");
		logger.info("[元数据获取]开始连接Oracle数据库获取元数据，约三分钟...");
		try {
			MetaData.getInstance().updateMaps();
			System.out.print(new Date(System.currentTimeMillis()) + ";");
			System.out.println("[元数据获取]连接Oracle数据库获取元数据完成");
			logger.info("[元数据创建]开始连接IoTDB数据库创建时间序列元数据");
			System.out.print(new Date(System.currentTimeMillis()) + ";");
			System.out.println("[元数据创建]开始创建IoTDB元数据");
			MetaData.getInstance().createStorageGroup();
			MetaData.getInstance().createTimeSeriesSchema();
			System.out.print(new Date(System.currentTimeMillis()) + ";");
			System.out.println("[数据写入]开始向IoTDB写入数据");
			ConsumerThreadV2 consumerThread = new ConsumerThreadV2();
			consumerThread.start(consumerNum);
		} catch (SQLException e) {
			System.out.println("[转换失败]连接Oracle数据库查找元数据失败");
			logger.info("[转换失败]连接Oracle数据库查找元数据失败");
			e.printStackTrace();
		}

    }

	public void launch(String args[]) throws IOException, AlreadyAliveException, InvalidTopologyException {
		System.out.println("[iotdb Debug]Entering launch()!!!");
		TopologyBuilder builder = new TopologyBuilder();
		Properties props = new Properties();
		JSONObject jsonObject;
		if(args.length > 1)
			jsonObject = JSONObject.parseObject(args[1]); 
		else
			jsonObject = JSONObject.parseObject("{"
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
        for (Object map : jsonObject.entrySet()){  
            props.put((String)((Map.Entry)map).getKey(), (String)((Map.Entry)map).getValue());  
        }  
		
		builder.setSpout("kafkaConsumer", new ConsumerSpout(),
				Integer.parseInt(props.getProperty("number_of_consumer")));
		builder.setBolt("IoTDB_store", new IoTDBTransferAndStoreBolt(), Integer.parseInt(props.getProperty("number_of_tranfer")))
				.shuffleGrouping("kafkaConsumer");

		
		Config conf = new Config();
		for (Map.Entry each : props.entrySet()) {
			conf.put((String) each.getKey(), each.getValue());
		}
		conf.setDebug(false);
		conf.registerSerialization(TransPacket.class);
		conf.setMaxSpoutPending(5000);
		conf.setMessageTimeoutSecs(30000);	
		conf.setNumAckers(3);
		
		// StormSubmitter.submitTopology("KmxStore", conf,
		// builder.createTopology());

		if (args != null && args.length > 0) {
			conf.setNumWorkers(Integer.parseInt(props.getProperty("workers")));
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			System.out.println(conf.toString());
			cluster.submitTopology("serili", conf, builder.createTopology());
			Utils.sleep(1000 * 60 * 600);// 运行600分钟后停止
			cluster.killTopology("serili");
			cluster.shutdown();
		}
	}

}
