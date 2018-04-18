package dao;

import dao.imp.KmxDao;

public class KmxDaoFactory {
	
	private static KmxDao dao;

	public synchronized static IKmxDAO getKMXDao(){
		if(dao == null){
			dao = new KmxDao();
		}
		return dao;
	}	
	
}
