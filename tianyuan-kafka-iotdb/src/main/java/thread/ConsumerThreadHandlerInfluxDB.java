package thread;


import conf.InfluxDBConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.influxdb.dto.BatchPoints;
import ty.pub.BeanUtil;
import ty.pub.ParsedPacketDecoder;
import ty.pub.TransPacket;
import util.InfluxDataModel;
import util.MetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;


public class ConsumerThreadHandlerInfluxDB implements Runnable {
    private String InfluxURL;
    private String InfluxDBName;
    private ConsumerRecord record;
    private final String DEFAULT_RP = "autogen";
    private static final String countSQL = "select count(%s) from %s where device='%s'";
    private org.influxdb.InfluxDB influxDB;
    int count = 0;
    long totalPoints = 0;
    long nullPoints = 0;

    public ConsumerThreadHandlerInfluxDB(ConsumerRecord consumerRecord) {
        this.record = consumerRecord;
        String INFLUX_URL="http://" + InfluxDBConfig.getInstance().getHost() + ":8086";
        String INFLUX_DB_NAME="kafka";
        InfluxURL = INFLUX_URL;
        InfluxDBName = INFLUX_DB_NAME;
        influxDB = org.influxdb.InfluxDBFactory.connect(InfluxURL);

        if (influxDB.databaseExists(InfluxDBName)) {
            influxDB.setDatabase(InfluxDBName);
        } else {
            influxDB.createDatabase(InfluxDBName);
        } //thread safety may need to be checked



    }

    private InfluxDataModel createDataModel(long timestamp, String series, String device, String sensor, Number value) {
        InfluxDataModel model = new InfluxDataModel();

        model.measurement = "type_" + series;
        model.tagSet.put("device", "d_" + device);

        model.timestamp = timestamp;
        model.fields.put(sensor, value);

        return model;
    }

    @Override
    public void run() {
        System.out.print(new Date(System.currentTimeMillis()) + ";");
        System.out.println("Consumer Message:" + record.value() + ",Partition:" + record.partition() + "Offset:" + record.offset());
        System.out.println("Current thread:" + Thread.currentThread().getName());
        TransPacket packet = new ParsedPacketDecoder().deserialize("", (byte[]) record.value());

        //flag = IoTDBInsert(tmp, count, statement,flag);

        //获取时间戳
        long time;
        try {
            time = getTimestamp(packet);
        } catch (Exception e) {
            System.out.println(BeanUtil.ObjectToString(packet));
            e.printStackTrace();
            return;
        }

        //存储数据
        Iterator<?> iterator = packet.getWorkStatusMapIter();
        String deviceNO = packet.getDeviceId();

        long errorNum = 0;

        String deviceSeries = MetaData.getInstance().getDeviceSeriesID(deviceNO);
        if(deviceSeries==null){
            System.out.println("null deviceSeries:"+deviceNO);
        }
        int nullNum = 0;
        boolean hasNull = false;
        if(influxDB != null) {
            BatchPoints batchPoints = BatchPoints.database(InfluxDBName).tag("async", "true").retentionPolicy(DEFAULT_RP)
                    .consistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL).build();
            while (iterator.hasNext()) {
                TransPacket.WorkStatusRecord entry = (TransPacket.WorkStatusRecord) iterator.next();
                String sensorName = entry.getWorkStatusId();
                String sensorValue = entry.getValue();
                long timeStamp = time + entry.getTime();

                String type = null;

                type = MetaData.getInstance().getSensorType(sensorName);
                if (type == null) {
                    System.out.println("null type:" + sensorName);
                }

                if (deviceSeries != null && type != null) {
                    String sql;
                    if (type.equals("String")) {
//                    sql = createInsertSQLStatment(
//                            Constants.STORAGE_GROUP_PREFIX + deviceSeries,
//                            Constants.DEVICE_PREFIX + deviceNO,
//                            Constants.SENSOR_PREFIX + sensorName,
//                            "\"" + sensorValue.replaceAll("\"", "\\\\\"").replaceAll("\'", "\\\\\'") + "\"",
//                            timeStamp);
                    } else {
                        Double sensorValue2Double = Double.parseDouble(sensorValue);
                        InfluxDataModel model = createDataModel(timeStamp, deviceSeries, deviceNO, sensorName, sensorValue2Double);
                        batchPoints.point(model.toInfluxPoint());
                        count++;
                        totalPoints++;
                    }
                } else {
                    nullPoints++;
                    nullNum++;
                    hasNull = true;
                }
            }
            try {
                System.out.print(new Date(System.currentTimeMillis()) + ";");
                System.out.println("[数据存储]InfluxDB执行批写入，语句数：" + count);
                influxDB.write(batchPoints);
                System.out.print(new Date(System.currentTimeMillis()) + ";");
                System.out.println("[数据存储]InfluxDB执行批写入:" + batchPoints.toString());
            } catch (Exception e) {
                System.out.print(new Date(System.currentTimeMillis()) + ";");
                System.out.println("[数据存储]InfluxDB批写入执行失败，失败数：" + errorNum);
                e.printStackTrace();
            }
            influxDB.close();
            if (hasNull) {
                System.out.print(new Date(System.currentTimeMillis()) + ";");
                System.out.println("[转换失败]设备系列或传感器类型为空：" + nullNum);
            }
        } else {
            System.out.print(new Date(System.currentTimeMillis()) + ";");
            System.out.println("[连接失败]InfluxDB连接失败");
        }
        System.out.println("[数据存储]类型为空占比："+100.0f * nullPoints / totalPoints + "%");
        influxDB.close();

    }


    private static long getTimestamp(TransPacket packet) {
        return Long.parseLong(packet.getBaseInfoMap().get("TY_0001_00_6"));
    }
}
