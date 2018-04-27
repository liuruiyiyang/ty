package thread;

import clojure.lang.Cons;
import conf.Constants;
import dao.IoTDBDaoFactory;
import dao.imp.IoTDBDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ty.pub.BeanUtil;
import ty.pub.ParsedPacketDecoder;
import ty.pub.TransPacket;
import util.MetaData;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Iterator;

public class ConsumerThreadHandler implements Runnable {
    private ConsumerRecord record;
    private static IoTDBDao dao = IoTDBDaoFactory.getIoTDBDao();
    private static final String DEFAULT_PATH_PREFIX = "root";
    int count = 0;
    long totalPoints = 0;
    long nullPoints = 0;
    Statement statement = null;


    public ConsumerThreadHandler(ConsumerRecord consumerRecord) {
        this.record = consumerRecord;

    }

    @Override
    public void run() {
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

        java.sql.Connection connection = dao.getConnection();
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            System.out.print(new Date(System.currentTimeMillis()) + ";");
            System.out.println("[转换失败]IoTDB连接失败");
        }

        String deviceSeries = MetaData.getInstance().getDeviceSeriesID(deviceNO);
        if(deviceSeries==null){
            System.out.println("null deviceSeries:"+deviceNO);
        }
        int nullNum = 0;
        boolean hasNull = false;
        if (statement != null) {
            while (iterator.hasNext()) {
                TransPacket.WorkStatusRecord entry = (TransPacket.WorkStatusRecord) iterator.next();
                String sensorName = entry.getWorkStatusId();
                String sensorValue = entry.getValue();
                long timeStamp = time + entry.getTime();

                String type = null;

                type = MetaData.getInstance().getSensorType(sensorName);
                if(type==null){
                    System.out.println("null type:"+sensorName);
                }

                if (deviceSeries != null && type != null) {
                    String sql;
                    if (type.equals("String")) {
                        sql = createInsertSQLStatment(
                                Constants.STORAGE_GROUP_PREFIX + deviceSeries,
                                Constants.DEVICE_PREFIX + deviceNO,
                                Constants.SENSOR_PREFIX + sensorName,
                                "\"" + sensorValue.replaceAll("\"", "\\\\\"").replaceAll("\'", "\\\\\'") + "\"",
                                timeStamp);
                    } else {
                        sql = createInsertSQLStatment(
                                Constants.STORAGE_GROUP_PREFIX + deviceSeries,
                                Constants.DEVICE_PREFIX + deviceNO,
                                Constants.SENSOR_PREFIX + sensorName,
                                sensorValue,
                                timeStamp);
                    }

                    try {
                        statement.addBatch(sql);
                        count++;
                        totalPoints++;
                    } catch (SQLException e) {
                        System.out.print(new Date(System.currentTimeMillis()) + ";");
                        System.out.println("[转换失败]IoTDB添加批写入语句失败");
                        e.printStackTrace();
                    }
                } else {
                    nullPoints++;
                    nullNum ++;
                    hasNull = true;
                }
            }

            try {
                System.out.print(new Date(System.currentTimeMillis()) + ";");
                System.out.println("[数据存储]IoTDB执行批写入，语句数：" + count);
                statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
                System.out.print(new Date(System.currentTimeMillis()) + ";");
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
            if(hasNull){
                System.out.print(new Date(System.currentTimeMillis()) + ";");
                System.out.println("[转换失败]设备系列或传感器类型为空："+nullNum);
            }
        }
        System.out.println("[数据存储]类型为空占比："+100.0f * nullPoints / totalPoints + "%");

    }

    private static String createInsertSQLStatment(String storageGroup, String device, String sensor, String value, long time) {

        StringBuilder builder = new StringBuilder();
        builder.append("insert into ").append(DEFAULT_PATH_PREFIX).append(".").append(storageGroup).append(".").append(device).append("(timestamp");
        builder.append(",").append(sensor);
        builder.append(") values(");
        builder.append(time);
        builder.append(",").append(value);
        builder.append(")");
        System.out.print(new Date(System.currentTimeMillis()) + ";");
        System.out.println("sql:   " + builder.toString());
        return builder.toString();
    }


    private static long getTimestamp(TransPacket packet) {
        return Long.parseLong(packet.getBaseInfoMap().get("TY_0001_00_6"));
    }
}
