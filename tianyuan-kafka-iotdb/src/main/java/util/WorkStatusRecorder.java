package util;

import java.io.FileOutputStream;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.Row;

import jdk.internal.dynalink.support.DefaultInternalObjectFilter;
import ty.pub.Error;

public class WorkStatusRecorder {

	private int index = 0;
	
	private int num;
	
	private final static int MAX_CACHE = 50000;
	
	private final static int MAX_PENDING = 500;
	
	HSSFWorkbook wb;
	
	HSSFSheet sheet;
	
	long time;
	
	Lock lock = new ReentrantLock();
	
	static WorkStatusRecorder workStatusRecorder;
	
	public static WorkStatusRecorder getWorkStatusRecorder(){
		if(workStatusRecorder == null)
			workStatusRecorder = new WorkStatusRecorder();
		return workStatusRecorder;
	}
	
	public WorkStatusRecorder(){
		init();
	}
	
	public void recordWorkStatus(WorkStatus workStatus){
		lock.lock();
		num++;
		Row row = sheet.createRow(num);
		row.createCell(0).setCellValue(workStatus.deviceId);
        row.createCell(1).setCellValue(workStatus.workStatusId);
        row.createCell(2).setCellValue(String.valueOf(workStatus.timestamp));
        row.createCell(3).setCellValue(workStatus.dataType);
        row.createCell(4).setCellValue(workStatus.value);
        if(num % 1000 == MAX_PENDING )
	        if(num >= MAX_CACHE){
	        	save();
	        }else{
	        	flush();
	        }
        lock.unlock();
	}
	
	private void init(){
		// 第一步，创建一个webbook，对应一个Excel文件  
        wb = new HSSFWorkbook();  
        // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet  
        sheet = wb.createSheet("工况数据详表");  
        // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short  
        Row row = sheet.createRow(0);        
        row.createCell(0).setCellValue("设备ID");
        row.createCell(1).setCellValue("工况Id");
        row.createCell(2).setCellValue("时间");
        row.createCell(3).setCellValue("类型");
        row.createCell(4).setCellValue("值");
        num = 0;
        time = new Date().getTime();
	}
	
	private void save(){
//		System.out.println("------------------保存第" + index + "个表---------------");
		try  
        {  
            FileOutputStream fout = new FileOutputStream("工况记录" + index++ + ".xls", false);  
            wb.write(fout);  
            fout.close();  
        }  
        catch (Exception e)  
        {  
            e.printStackTrace();  
        }finally{
        	init();
        }
	}
	
	private void flush(){
		if(new Date().getTime() - time > 10000){
			System.out.println("------------------刷新第" + index + "个表---------------");
			try  
	        {  
	            FileOutputStream fout = new FileOutputStream("工况记录" + index + ".xls", false);  
	            wb.write(fout);  
	            fout.close();  
	        }  
	        catch (Exception e)  
	        {  
	            e.printStackTrace();  
	        }finally {
				time = new Date().getTime();
			}
		}
	}
	
	protected void finalize() throws Throwable{
		super.finalize();
		save();
	}
	
	
	
}
