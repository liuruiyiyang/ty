package util;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MetaDataTest {
    // The recommended format of a connection URL is the long format with the
    // connection descriptor.
    final static String DB_URL= "jdbc:oracle:thin:@192.168.10.50:1521/ptmro";
    final static String DB_USER = "monitor211";
    final static String DB_PASSWORD = "mn";

    private static HashMap<String, String> seriesIDMap = new HashMap<>();
    private static HashMap<String, String> invSeriesIDMap = new HashMap<>();
    private static ArrayList<String> seriesIDList = new ArrayList<>();
    private static HashMap<String, String> sensorTypeMap = new HashMap<>();

    /*
     * The method gets a database connection using
     * oracle.jdbc.pool.OracleDataSource. It also sets some connection
     * level properties, such as,
     * OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH,
     * OracleConnection.CONNECTION_PROPERTY_THIN_NET_CHECKSUM_TYPES, etc.,
     * There are many other connection related properties. Refer to
     * the OracleConnection interface to find more.
     */
    public static void main(String args[]) throws SQLException {
        Properties info = new Properties();
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

        long start = System.currentTimeMillis();

        // With AutoCloseable, the connection is closed automatically.
        try (OracleConnection connection = (OracleConnection) ods.getConnection()) {
            // Get the JDBC driver name and version
            DatabaseMetaData dbmd = connection.getMetaData();
            System.out.println("Driver Name: " + dbmd.getDriverName());
            System.out.println("Driver Version: " + dbmd.getDriverVersion());
            // Print some connection properties
            System.out.println("Default Row Prefetch Value is: " +
                    connection.getDefaultRowPrefetch());
            System.out.println("Database Username is: " + connection.getUserName());
            System.out.println();
            // Perform a database operation

            //printTest(connection);

            // Statement and ResultSet are AutoCloseable and closed automatically.

        }

        createTimeSeriesSchemaTest();

/*
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
                        ResultSetMetaData rsmd = resultSet.getMetaData();
                        int clumnNum = rsmd.getColumnCount();
                        System.out.println("clumnNum="+clumnNum);

                        ArrayList<String> names = new ArrayList<>();
                        for(int i =1;i<=clumnNum;i++){
                            names.add(rsmd.getColumnName(i));
                        }
                        for(String s :names ){
                            System.out.print(s + "  ");
                        }
                        System.out.println();
                        System.out.println("---------------------");
                        while (resultSet.next()) {
                            for(int i =1;i<=clumnNum;i++) {
                                System.out.print(resultSet.getString(i) + "   ");
                            }

                            System.out.println();
                            sensorTypeMap.put(resultSet.getString(2), resultSet.getString(3));
                        }
                    }
                }
            }
        }
*/
/*
        for(Map.Entry<String,String> entry: seriesIDMap.entrySet()){
            System.out.println("entry.getKey()="+entry.getKey()+",entry.getValue()="+entry.getValue());
        }

        long end = System.currentTimeMillis();

        System.out.println("sensorTypeMap Size = "+ seriesIDMap.size());
        System.out.println("Time = "+ (end-start)/1000.0);
*/
    }


    public static void printTest(Connection connection) throws SQLException {
        // Statement and ResultSet are AutoCloseable and closed automatically.
        //String sql = "select PLT_DEVICESERIESID  from plt_tsm_device WHERE PLT_DEVICENO=\'1001161790\'";
//        String sql = "select distinct dt.plt_leftoid,dt.plt_parameterid,c.plt_name " +
//                "       from plt_cus_r_deviceser2temppara dt " +
//                "       join plt_tsm_templatepara tp on dt.plt_rightoid = tp.plt_oid" +
//                "       join plt_mpm_code c on c.plt_subcode = tp.plt_infotype" +
//                "       where dt.plt_leftoid='77BD885EB46E984B9FA64F758430ED44' and c.plt_majorcode = 24 ";
        //String sql = "select PLT_DEVICESERIESID, PLT_DEVICENO  from plt_tsm_device ";
//        String sql = "select distinct dt.plt_leftoid,dt.plt_parameterid,c.plt_name " +
//                "       from plt_cus_r_deviceser2temppara dt " +
//                "       join plt_tsm_templatepara tp on dt.plt_rightoid = tp.plt_oid" +
//                "       join plt_mpm_code c on c.plt_subcode = tp.plt_infotype" +
//                "       where dt.plt_leftoid=\'" + "77BD885EB46E984B9FA64F758430ED44" + "\' and c.plt_majorcode = 24 ";
        //String sql = "select PLT_PARAMETERID, PLT_INFOTYPE  from plt_tsm_templatepara where plt_parameterid = \'TY_0004_00_31\' ";

        String sql = "select DISTINCT PLT_OID  from plt_tsm_deviceseries ";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement
                    .executeQuery(sql)) {
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int clumnNum = rsmd.getColumnCount();
                System.out.println("clumnNum="+clumnNum);

                ArrayList<String> names = new ArrayList<>();
                for(int i =1;i<=clumnNum;i++){
                    names.add(rsmd.getColumnName(i));
                }
                for(String s :names ){
                    System.out.print(s + "  ");
                }
                System.out.println();
                System.out.println("---------------------");
                while (resultSet.next()){
                    for(int i =1;i<=clumnNum;i++) {
                        System.out.print(resultSet.getString(i) + "   ");
                    }

                    System.out.println();
                }

            }
        }
    }

    private static void createTimeSeriesSchemaTest(){
        System.out.println("[元数据获取]开始连接Oracle数据库获取元数据，约三分钟...");
        try {
            MetaData.getInstance().updateMaps();
        } catch (SQLException e) {
            System.out.println("[转换失败]连接Oracle数据库查找元数据失败");
            e.printStackTrace();
        }
        System.out.println("[元数据获取]连接Oracle数据库获取元数据完成");
        MetaData.getInstance().createTimeSeriesSchema();
    }

}
