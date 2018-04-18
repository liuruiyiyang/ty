package dao.imp;

import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import com.sagittarius.bean.common.ValueType;
import conf.IoTDBConfig;
import org.apache.log4j.Logger;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class IoTDBDao {
    static Logger logger = Logger.getLogger(IoTDBDao.class);
    static final int SYNC_INTERVAL = 180000;
    private java.sql.Connection connection;
    private static final String IOTDB_URL = "jdbc:tsfile://%s:%s/";
    long timestamp;
    Map<String, Map<String, ValueType>> deviceTypesAndSensor;

    //device -> devicetype
    Map<String, String> deviceTypeAndDevices;

    Set<String> missing;

    public IoTDBDao() throws ClassNotFoundException{
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        boolean notEnd = true;
        while(notEnd){
            try {
                init();
                notEnd = false;
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public void init()  {

        IoTDBConfig config =  IoTDBConfig.builder().build();
        try {
            connection = DriverManager.getConnection(String.format(IOTDB_URL, config.getHost(), config.getPort()), config.getUser(),
                    config.getPasswd());
        } catch (SQLException e) {
            System.out.println("IoTDB连接失败");
            e.printStackTrace();
        }
    }

    public java.sql.Connection getConnection(){
        return this.connection;
    }

}
