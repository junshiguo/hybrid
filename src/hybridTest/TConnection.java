package hybridTest;

import java.sql.Connection;

import utility.DBManager;

public class Request extends Thread {
	public int tenantId;
	public String mysqlURL;
	public String mysqlUsername;
	public String mysqlPassword;
	public Connection connection;
	
	public Request(int id, String url, String username, String password){
		this.tenantId = id;
		this.mysqlURL = url;
		this.mysqlUsername = username;
		this.mysqlPassword = password;
		connection = DBManager.connectDB(url, username, password);
	}
	
	public boolean doSQL(int sqlId, Object[] para, Object[] paraType){
		return false;
	}

}
