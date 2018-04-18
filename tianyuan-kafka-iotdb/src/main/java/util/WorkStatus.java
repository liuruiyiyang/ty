package util;

public class WorkStatus {

	String deviceId;
	
	String workStatusId;
	
	long timestamp;
	
	String dataType;
	
	String value;

	public WorkStatus(String deviceId, String workStatusId, long timestamp, String dataType, String value) {
		super();
		this.deviceId = deviceId;
		this.workStatusId = workStatusId;
		this.timestamp = timestamp;
		this.dataType = dataType;
		this.value = value;
	}
	
}
