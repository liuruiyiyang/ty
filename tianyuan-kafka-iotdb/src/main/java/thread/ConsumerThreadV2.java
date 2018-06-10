package thread;

import com.alibaba.fastjson.JSONObject;
import conf.Constants;
import conf.InfluxDBConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerThreadV2 {
    private static org.apache.kafka.clients.consumer.KafkaConsumer<String,  byte[]> consumer;
    private ExecutorService executor;

    public ConsumerThreadV2(){
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
        System.out.print(new Date(System.currentTimeMillis()) + ";");
        System.out.println("ConsumerThreadV2 初始化完毕");

    }

    public void start(int threadNumber, String db_name){
        System.out.print(new Date(System.currentTimeMillis()) + ";");
        System.out.println("ConsumerThreadV2 执行 start(), delay =" + InfluxDBConfig.getInstance().getDelay());
        executor = new ThreadPoolExecutor(threadNumber,threadNumber,0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        if(db_name.equals(Constants.IOTDB)) {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for (ConsumerRecord<String, byte[]> record : records) {
                    executor.submit(new ConsumerThreadHandler(record));
                }
            }
        } else {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for (ConsumerRecord<String, byte[]> record : records) {
                    executor.submit(new ConsumerThreadHandlerInfluxDB(record));

                    try {
                        Thread.sleep(InfluxDBConfig.getInstance().getDelay());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

            }
        }
    }
}
