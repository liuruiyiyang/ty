package conf;

import com.sagittarius.write.internals.BasicMonitor;
import com.sagittarius.write.internals.Monitor;


public class  IoTDBConfig {
    private String host;
    private String port;
    private String user;
    private String passwd;
    private int batchSize;
    private Monitor monitor;

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPasswd() {
        return passwd;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public static IoTDBConfig.Builder builder(){
        return new IoTDBConfig.Builder();
    }

    private IoTDBConfig(){
        this.monitor = new BasicMonitor();
        //this.host = "192.168.10.57";
        //this.host = "166.111.7.246";
        this.host = "192.168.130.20";
        this.port = "6667";
        this.user = "root";
        this.passwd = "root";
        this.batchSize = 1000;
    }


    public static class Builder {
        private IoTDBConfig config = new IoTDBConfig();

        public Builder(){
            
        }

        public IoTDBConfig build(){
            return this.config;
        }

        public IoTDBConfig.Builder setMonitor(Monitor monitor){
            this.config.monitor = monitor;
            return this;
        }

        public IoTDBConfig.Builder setHost(String host){
            this.config.host = host;
            return this;
        }

        public IoTDBConfig.Builder setPort(String port){
            this.config.port = port;
            return this;
        }

        public IoTDBConfig.Builder setUser(String user){
            this.config.user = user;
            return this;
        }

        public IoTDBConfig.Builder setPassWD(String passwd){
            this.config.passwd = passwd;
            return this;
        }

        public IoTDBConfig.Builder setBatchSize(int batchSize){
            this.config.batchSize = batchSize;
            return this;
        }
    }
}
