package util;

import conf.Constants;
import dao.IoTDBDaoFactory;
import dao.imp.IoTDBDao;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class MetaData {
    static Logger logger = Logger.getLogger(MetaData.class);
    private static IoTDBDao dao = IoTDBDaoFactory.getIoTDBDao();
    final static String DB_URL = "jdbc:oracle:thin:@192.168.10.50:1521/ptmro";
    final static String DB_USER = "monitor211";
    final static String DB_PASSWORD = "mn";
    private static HashMap<String, String> seriesIDMap = new HashMap<>();
    private static ArrayList<String> seriesIDList = new ArrayList<>();
    private static HashMap<String, String> sensorTypeMap = new HashMap<>();
    private static HashMap<String,HashMap<String,String>> seriesSensorTypeMap = new HashMap<>();
    private static HashMap<String, HashMap<String, String>> typeEncodingMap = new HashMap<>();
    private static final String createStatementSQL = "create timeseries %s with datatype=%s, encoding=%s";
    private static final String setStorageLevelSQL = "set storage group to %s";
    private final String ENCODING = "ENCODING";
    private final String DATA_TYPE = "DATA_TYPE";
    private final int CREATE_SCHEMA_BATCH_SIZE = 1000;
    private Statement statement = null;

    private static class MetaDataHolder {
        private static final MetaData INSTANCE = new MetaData();
    }

    public static final MetaData getInstance() {
        return MetaDataHolder.INSTANCE;
    }

    MetaData(){
        createTypeEncodingMap();
    }

    public static String getDeviceSeriesID(String deviceNO) {
        return seriesIDMap.get(deviceNO);
    }

    public static String getSensorType(String sensor) {
        return sensorTypeMap.get(sensor);
    }

    public static void updateMaps() throws SQLException {
        Properties info = new Properties();
        //System.out.println("getSensorType()begin");
        info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
        info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);
        info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");
        info.put(OracleConnection.CONNECTION_PROPERTY_THIN_NET_CHECKSUM_TYPES,
                "(MD5,SHA1,SHA256,SHA384,SHA512)");
        info.put(OracleConnection.CONNECTION_PROPERTY_THIN_NET_CHECKSUM_LEVEL,
                "REQUIRED");

        OracleDataSource ods = new OracleDataSource();
        ods.setURL(DB_URL);
        ods.setConnectionProperties(info);

        try (OracleConnection connection = (OracleConnection) ods.getConnection()) {
            // Statement and ResultSet are AutoCloseable and closed automatically.
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement
                        .executeQuery("select PLT_DEVICESERIESID, PLT_DEVICENO  from plt_tsm_device ")) {
                    while (resultSet.next()) {
                        seriesIDMap.put(resultSet.getString(2), resultSet.getString(1));
                    }
                }
            }
        }

        try (OracleConnection connection = (OracleConnection) ods.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement
                        .executeQuery("select DISTINCT PLT_OID  from plt_tsm_deviceseries ")) {
                    while (resultSet.next()) {
                        seriesIDList.add(resultSet.getString(1));
                    }
                }
            }
        }

        for (String seriesID : seriesIDList) {
            String sql = "select distinct dt.plt_leftoid,dt.plt_parameterid,c.plt_name " +
                    "       from plt_cus_r_deviceser2temppara dt " +
                    "       join plt_tsm_templatepara tp on dt.plt_rightoid = tp.plt_oid" +
                    "       join plt_mpm_code c on c.plt_subcode = tp.plt_infotype" +
                    "       where dt.plt_leftoid=\'" + seriesID + "\' and c.plt_majorcode = 24 ";

            try (OracleConnection connection = (OracleConnection) ods.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet resultSet = statement
                            .executeQuery(sql)) {
                        while (resultSet.next()) {
                            String SeriesID = resultSet.getString(1);
                            String ParameterID = resultSet.getString(2);
                            String ParameterType = resultSet.getString(3);
                            if(seriesSensorTypeMap.containsKey(SeriesID)){
                                seriesSensorTypeMap.get(SeriesID).put(ParameterID, ParameterType);
                            } else {
                                HashMap<String, String> map = new HashMap<>();
                                map.put(ParameterID,ParameterType);
                                seriesSensorTypeMap.put(SeriesID, map);
                            }

                            sensorTypeMap.put(ParameterID, ParameterType);
                        }
                    }
                }
            }
        }

    }

    public void createStorageGroup(){
        System.out.println("[元数据创建]开始连接IoTDB创建存储组");
        java.sql.Connection connection = dao.getConnection();
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            System.out.println("[元数据创建失败]IoTDB连接失败");
        }
        if(statement!=null) {
            for (String seriesID : seriesIDList) {
                String storageGroup = Constants.ROOT + "." + Constants.STORAGE_GROUP_PREFIX + seriesID;
                String sql = String.format(setStorageLevelSQL, storageGroup);
                try {
                    statement.execute(sql);
                    System.out.print(new Date(System.currentTimeMillis()) + ";");
                    System.out.println("sql: " + sql);
                    logger.info("[元数据创建]执行存储组创建语句:" + sql);
                } catch (SQLException e) {
                    System.out.print(new Date(System.currentTimeMillis()) + ";");
                    System.out.println("[元数据创建失败]存储组创建语句执行失败:" + sql);
                    logger.info("[元数据创建失败]存储组创建语句执行失败:" + sql);
                    e.printStackTrace();
                }
            }
            System.out.println("[元数据创建]连接IoTDB创建存储组完成");
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Can`t close statement when setting storage group");
            }
        }else{
            System.out.println("[元数据创建失败]IoTDB连接statement为null");
        }

    }

    public void createTimeSeriesSchema(){
        java.sql.Connection connection = dao.getConnection();
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            System.out.println("[元数据创建失败]IoTDB连接失败");
        }
        long count = 0;
        for(Map.Entry<String,String> entry: seriesIDMap.entrySet()){
            String root_group_device_path = Constants.ROOT + "." + Constants.STORAGE_GROUP_PREFIX + entry.getValue() + "." + Constants.DEVICE_PREFIX + entry.getKey();
            if(seriesSensorTypeMap.get(entry.getValue()) != null) {
                for (Map.Entry<String, String> entry1 : seriesSensorTypeMap.get(entry.getValue()).entrySet()) {
                    String root_group_device_sensor_path = root_group_device_path + "." + Constants.SENSOR_PREFIX + entry1.getKey();
                    String SQL = String.format(
                            createStatementSQL,
                            root_group_device_sensor_path,
                            typeEncodingMap.get(entry1.getValue()).get(DATA_TYPE),
                            typeEncodingMap.get(entry1.getValue()).get(ENCODING)
                    );
                    count++;
                    createTimeSeriesBatch(SQL, count, statement);
                    System.out.println("time Series count = " + count);
                    logger.info("time Series count = " + count);
                    logger.info("create schema sql: " + SQL);
                    System.out.print(new Date(System.currentTimeMillis()) + ";");
                    System.out.println("create schema sql: " + SQL);
                }
            } else {
                System.out.println("can not find " + entry.getValue() + " in seriesSensorTypeMap.");
                logger.error("can not find " + entry.getValue() + " in seriesSensorTypeMap.");
            }
        }
        try {
            statement.executeBatch();
            statement.clearBatch();
            statement.close();
        } catch (SQLException e) {
            System.out.println("error occurred in the last batch");
            logger.warn("error occurred in the last batch");
        }
    }

    private void createTimeSeriesBatch(String sql, long count, Statement statement) {

        if ((count % CREATE_SCHEMA_BATCH_SIZE) == 0) {

            try {
                statement.executeBatch();
            } catch (SQLException e) {
                System.out.println("Can`t execute batch when creating timeseries ");
                logger.warn("Can`t execute batch when creating timeseries ");
            }
            try {
                statement.clearBatch();
            } catch (SQLException e) {
                System.out.println("Can`t clear batch when creating timeseries ");
                logger.warn("Can`t clear batch when creating timeseries ");
            }

        } else  {
            try {
                statement.addBatch(sql);
            } catch (SQLException e) {
                System.out.println("Can`t add batch when creating timeseries");
                logger.warn("Can`t add batch when creating timeseries");
            }
        }
    }

    private void createTypeEncodingMap(){
        HashMap<String, String> floatTypeEncodingMap = new HashMap<>();
        floatTypeEncodingMap.put(ENCODING, "GORILLA");
        floatTypeEncodingMap.put(DATA_TYPE, "FLOAT");
        typeEncodingMap.put("Float",floatTypeEncodingMap);

        HashMap<String, String> doubleTypeEncodingMap = new HashMap<>();
        doubleTypeEncodingMap.put(ENCODING, "GORILLA");
        doubleTypeEncodingMap.put(DATA_TYPE, "DOUBLE");
        typeEncodingMap.put("Double",doubleTypeEncodingMap);


        HashMap<String, String> longTypeEncodingMap = new HashMap<>();
        longTypeEncodingMap.put(ENCODING, "RLE");
        longTypeEncodingMap.put(DATA_TYPE, "INT64");
        typeEncodingMap.put("Long",longTypeEncodingMap);



        HashMap<String, String> intTypeEncodingMap = new HashMap<>();
        intTypeEncodingMap.put(ENCODING, "RLE");
        intTypeEncodingMap.put(DATA_TYPE, "INT32");
        typeEncodingMap.put("Int",intTypeEncodingMap);

        HashMap<String, String> stringTypeEncodingMap = new HashMap<>();
        stringTypeEncodingMap.put(ENCODING, "PLAIN");
        stringTypeEncodingMap.put(DATA_TYPE, "TEXT");
        typeEncodingMap.put("String",stringTypeEncodingMap);

    }

}
