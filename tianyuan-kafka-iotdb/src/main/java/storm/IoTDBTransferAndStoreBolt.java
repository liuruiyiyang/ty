package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import com.sagittarius.write.SagittariusWriter;
import dao.Connection;

import dao.IoTDBDaoFactory;

import dao.imp.IoTDBDao;
import org.apache.log4j.Logger;
import ty.pub.BeanUtil;
import ty.pub.TransPacket;
import util.MetaData;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;

public class IoTDBTransferAndStoreBolt extends BaseRichBolt {
    static Logger logger = Logger.getLogger(IoTDBTransferAndStoreBolt.class);
    private static final long serialVersionUID = 16546L;
    private final String DEFAULT_PATH_PREFIX = "root";
    private static final String createStatementSQL = "create timeseries %s with datatype=TEXT,encoding=PLAIN";
    private static final String setStorageLevelSQL = "set storage group to %s";
    private OutputCollector _collector;

    private String raw;
    private TransPacket packet ;

    private String stampTimeID ="1";

    IoTDBDao dao;
    SagittariusWriter.Data data;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        logger.info("[iotdb Debug]Entering prepare()");
        System.out.println("[iotdb Debug]Entering prepare()");
        _collector = collector;
        Connection.init(stormConf);
        raw = (String) stormConf.get("decodeData");
        stampTimeID = (String) stormConf.get("stampTime_ID");
    }

    public void execute(Tuple input) {
        logger.info("[iotdb Debug]Entering execute()");
        System.out.println("[iotdb Debug]Entering execute()");
        //接收原始数据包
        try {
            packet  = KryoUtil.deserialize((byte[])input.getValue(0), TransPacket.class);
        } catch (Exception e) {
            System.out.println("[转换失败]Byte转化为TransPacket失败");
        }

        IoTDBInsert(packet);

        _collector.ack(input);
    }

    public void IoTDBInsert(TransPacket packet) {
        System.out.println("[iotdb debug]Entering IoTDBInsert()");
        //连接IoTDB
        dao = IoTDBDaoFactory.getIoTDBDao();


        //获取时间戳
        long time;
        try {
            time = getTimestamp(packet);
        } catch (Exception e) {
            System.out.println(BeanUtil.ObjectToString(packet));
            e.printStackTrace();
            return;
        }

        //获取metadata
        Iterator<?> iteratorInfo = packet.getBaseInfoMapIter();


        //存储数据
        Statement statement = null;
        long errorNum = 0;
        java.sql.Connection connection = dao.getConnection();
        try {
            statement = connection.createStatement();
        } catch (SQLException e){
            System.out.println("[转换失败]IoTDB连接失败");
        }


        Iterator<?> iterator = packet.getWorkStatusMapIter();
        if(statement != null) {
            while (iterator.hasNext()) {
                TransPacket.WorkStatusRecord entry = (TransPacket.WorkStatusRecord) iterator.next();
                String deviceSeries = null;

                deviceSeries = MetaData.getDeviceSeriesID(packet.getDeviceId());

                if(deviceSeries!=null) {
                    try {
                        statement.execute(String.format(setStorageLevelSQL, "root."+"type_" + deviceSeries));
                        System.out.println("setStorageLevelSQL:" + String.format(setStorageLevelSQL, "root."+"type_" + deviceSeries));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                    try {
                        System.out.println("createTSSQL:" + String.format(createStatementSQL,
                                "root."+"type_" + deviceSeries+ "." + "d_" + packet.getDeviceId() + "." + "s_" + entry.getWorkStatusId()));

                        statement.execute(String.format(createStatementSQL,
                                "root."+"type_" + deviceSeries+ "." + "d_" + packet.getDeviceId() + "." + "s_" + entry.getWorkStatusId()));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                    String sql = createInsertSQLStatment("type_" + deviceSeries, "d_" + packet.getDeviceId(), "s_" + entry.getWorkStatusId(), "\'" + entry.getValue() + "\'", time + entry.getTime());
                    try {
                        statement.addBatch(sql);
                    } catch (SQLException e) {
                        System.out.println("[转换失败]IoTDB添加批写入语句失败");
                        e.printStackTrace();
                    }
                }
            }
            try {
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
        }
    }

    private String createInsertSQLStatment(String storageGroup, String device, String sensor, String value, long time) {



        StringBuilder builder = new StringBuilder();
        builder.append("insert into ").append(DEFAULT_PATH_PREFIX).append(".").append(storageGroup).append(".").append(device).append("(timestamp");
        builder.append(",").append(sensor);
        builder.append(") values(");
        builder.append(time);
        builder.append(",").append(value);
        builder.append(")");
        System.out.println("Insert SQL:" + builder.toString());
        return builder.toString();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private long getTimestamp(TransPacket packet) {
        return Long.parseLong(packet.getBaseInfoMap().get(stampTimeID));
    }

}
