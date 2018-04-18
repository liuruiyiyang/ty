package dao;

import dao.imp.IoTDBDao;

public class IoTDBDaoFactory {
    private static IoTDBDao dao;

    public synchronized static IoTDBDao getIoTDBDao(){
        if(dao == null){
            try {
                dao = new IoTDBDao();
            } catch (ClassNotFoundException e) {
                System.out.println("[转换失败]IoTDB连接失败");
                e.printStackTrace();
            }
        }
        return dao;
    }
}
