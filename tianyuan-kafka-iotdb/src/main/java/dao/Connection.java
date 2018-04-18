package dao;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.log4j.Logger;

//import org.apache.spark.SparkConf;

import com.datastax.driver.core.ConsistencyLevel;

import transfer.ITransfer;
import tsinghua.thss.sdk.core.Client;
import tsinghua.thss.sdk.core.KMXClient;
import tsinghua.thss.sdk.core.KMXConfig;
import tsinghua.thss.sdk.read.Reader;
import tsinghua.thss.sdk.write.Writer;

public class Connection {
	
	static Logger logger = Logger.getLogger(Connection.class);

	private static Client client;
	
	private static Map<String, String> props;
	
	private static int count = 0;
	
	private static int MAX_Writer = 0;
	
	private static Queue<Writer> writers = new LinkedList<Writer>();
	
	private static Writer staticWriter;
	
	private static boolean writerCache = false;
	
	public synchronized static Writer getWriter(){
		if(client == null){
			init();
		}
		if(writerCache){
			Writer writer;
			if(count < MAX_Writer){
				count++;
				writer = client.getWriter();
			}else{
				writer = writers.poll();
			}
			writers.add(writer);
			return writer;
		}else{
			return staticWriter;
		}
	}
	
	public static Reader getReader(){
		if(client == null){
			init();
		}
		return client.getReader();
	}
	
	private static void init(){
		if(props == null)
			logger.error("连接初始化失败");
		//配置KMXConfig
		KMXConfig config = KMXConfig.builder()
	                	.setClusterNodes(props.get("kmx_url").split(","))
//		                .setClusterNodes(new String[]{"192.168.15.115","192.168.15.114","192.168.15.119"})		                
		                .setClusterPort(9042)
		                .setCoreConnectionsPerHost(3)
		                .setMaxConnectionsPerHost(10)
		                .setMaxRequestsPerConnection(4096)
		                .setHeartbeatIntervalSeconds(0)
		                .setTimeoutMillis(12000)
		                .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE) 
//		                .setSparkConf(sparkConf)
		                .setCacheSize(10000)
		                .setAutoBatch(false)
		                .setBatchSize(3000)
		                .setLingerMs(2)
		                .setMonitor(new LogMonitor())
		                .setConsistencyLevel(ConsistencyLevel.TWO)
		                .build();
		client = new KMXClient(config); 
		
		MAX_Writer = Integer.parseInt(props.get("kmx_max_writer_num"));
		
		writerCache = Boolean.parseBoolean(props.get("writer_cache"));
	
		if(!writerCache)
			staticWriter = client.getWriter();
		
		ITransfer.saveFlag = Boolean.parseBoolean(props.get("save_flag"));
	}
	
	public static void init(Map<String, String> p){
		logger.info(p.toString());
		props = p;
		client = null;
	}
	
	public static void main(String args[]){
//		SparkConf sparkConf = new SparkConf();
//		sparkConf.setMaster("spark://192.168.15.114:7077").setAppName("kmx");
//		sparkConf.set("spark.cores.max", "20");
//		sparkConf.set("spark.ui.port", "4041");
//		sparkConf.set("spark.executor.memory", "2g");

		//配置KMXConfig
		KMXConfig config = KMXConfig.builder()
		                .setClusterNodes(new String[]{"192.168.15.115","192.168.15.114","192.168.15.119"})
		                .setClusterPort(9042)
		                .setCoreConnectionsPerHost(3)
		                .setMaxConnectionsPerHost(10)
		                .setMaxRequestsPerConnection(4096)
		                .setHeartbeatIntervalSeconds(0)
		                .setTimeoutMillis(12000)
		                .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE) 
//		                .setSparkConf(sparkConf)
		                .setCacheSize(10000)
		                .setAutoBatch(true)
		                .setBatchSize(3000)
		                .setLingerMs(2)
		                .setMonitor(new LogMonitor())
		                .build();
		client = new KMXClient(config);
	}
	
	public static void reInit(){
		if(props == null)
			logger.error("连接初始化失败");
		//配置KMXConfig
		KMXConfig config = KMXConfig.builder()
	                	.setClusterNodes(props.get("kmx_url").split(","))
//		                .setClusterNodes(new String[]{"192.168.15.115","192.168.15.114","192.168.15.119"})		                
		                .setClusterPort(9042)
		                .setCoreConnectionsPerHost(3)
		                .setMaxConnectionsPerHost(10)
		                .setMaxRequestsPerConnection(4096)
		                .setHeartbeatIntervalSeconds(0)
		                .setTimeoutMillis(12000)
		                .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE) 
//		                .setSparkConf(sparkConf)
		                .setCacheSize(10000)
		                .setAutoBatch(false)
		                .setBatchSize(3000)
		                .setLingerMs(2)
		                .setMonitor(new LogMonitor())
		                .setConsistencyLevel(ConsistencyLevel.TWO)
		                .build();
		client = new KMXClient(config); 
	
		if(!writerCache)
			staticWriter = client.getWriter();
	}
}
