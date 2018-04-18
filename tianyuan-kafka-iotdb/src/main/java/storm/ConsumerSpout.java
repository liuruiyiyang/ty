package storm;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.kafka.common.TopicPartition;

public class ConsumerSpout extends BaseRichSpout {
	
	SpoutOutputCollector _collector;
	//已发送数据
    private ConcurrentHashMap<UUID, Values> _pending;  

//	consumer = new KafkaConsumer<String, byte[]>(props);
	
	KafkaConsumer<String,  byte[]> consumer;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		System.out.println("[init Consumer]-------------");
		_collector = collector;
		_pending = new ConcurrentHashMap<UUID, Values>(); 
		
		consumer = new KafkaConsumer<String, byte[]>(conf);

		consumer.subscribe(Arrays.asList((String)conf.get("topic")));

		/*
		ConsumerRecords<String, byte[]> records = consumer.poll(1000);
		for (ConsumerRecord<String, byte[]> record : records) {
			System.out.println(record);
			consumer.seekToBeginning(Collections.singleton(new TopicPartition(record.topic(), record.partition())));
		}
		*/
		consumer.poll(100);
		consumer.seekToBeginning(consumer.assignment());
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		
		ConsumerRecords<String, byte[]> records = consumer.poll(100);
		for (ConsumerRecord<String, byte[]> record : records){
	
//			System.out.println("kafka偏移量："+record.partition() + "-" +record.offset());
			
			//spout对发射的tuple进行缓存  
//			String msgId = record.partition() + "-" +record.offset();
			
			UUID msgId = UUID.randomUUID(); 
			Values v = new Values((byte[])record.value());
		    this._pending.put(msgId,v);
		    
		    //发射tuple时，添加msgId  
			_collector.emit(v,msgId);
		}
	}

	@Override  
    public void ack(Object msgId) {  
		//对于成功处理的tuple从缓存队列中删除
		this._pending.remove(msgId);  
    }  
  
    @Override  
    public void fail(Object msgId) {
    	//当消息处理失败了，重新发射，也可以做其他的逻辑处理 	
    	this._collector.emit(this._pending.get(msgId),msgId);
    }  
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("data"));
	}

}
