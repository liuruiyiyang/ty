package dao;

import java.util.ArrayList;


import com.sagittarius.bean.result.BatchPoint;
import com.sagittarius.write.internals.Monitor;

import org.apache.log4j.Logger;

public class LogMonitor implements Monitor{

	private static Logger logger = Logger.getLogger(LogMonitor.class);
	
	boolean isStop = false;
	
	public void inform(ArrayList<BatchPoint> points) {
		// TODO Auto-generated method stub
		if(!isStop){
			for(BatchPoint point : points){
				logger.error(String.format("[数据保存失败]\n车号：%s，工况号：%s，时间：%s，数据类型：%s", 
						point.getHost(), point.getMetric(), point.getPrimaryTime(), point.getValueType().name()));
			}
		}
	}

	public void stop() {
		// TODO Auto-generated method stub
		isStop = true;
	}

}
