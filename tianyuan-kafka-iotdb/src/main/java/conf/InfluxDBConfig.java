package conf;

public class InfluxDBConfig {
    private String host;


    private static class InfluxDBConfigHolder{
        private static final InfluxDBConfig INSTANCE = new InfluxDBConfig();
    }

    public static final InfluxDBConfig getInstance(){
        return InfluxDBConfigHolder.INSTANCE;
    }

    private InfluxDBConfig(){
        //this.host = "192.168.10.57";
        //this.host = "166.111.7.246";
        //this.host = "192.168.130.20";
        this.host = "127.0.0.1";
    }

    public void setHost(String h){
        this.host = h;
    }

    public String getHost(){
        return this.host;
    }
}
