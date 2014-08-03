package hybridTest;

import java.sql.SQLException;

import utility.Sequence;
import hybridConfig.HConfig;

public class HTenant extends Thread {
	public int tenantId;
	public double dataSize;
	public int QT;
	public int actualQT;  //actual throughput per minute
	public boolean writeHeavy;
	public int doSQLNow = 0;
	public int idInVoltdb = -1;
	public int base;
	public int bonus;
	
	public String dbURL;
	public String dbUsername;
	public String dbPassword;
	public String voltdbServer;
	public HConnection connection;
	public Sequence sequence;
	public Driver driver;
	
	public HTenant(int id, double ds, int qt, boolean wh,  String url, String username, String password, String voltdbServer){
		this.tenantId = id;
		this.dataSize = ds;
		this.QT = qt; 
		this.base = 0;
		this.bonus = 0;
		this.writeHeavy = wh;
		this.dbURL = url;
		this.dbUsername = username;
		this.dbPassword = password;
		this.voltdbServer = voltdbServer;
		connection = new HConnection(0, tenantId, url, username, password, voltdbServer);
		sequence = new Sequence();
		if(writeHeavy){
			sequence.initSequence(HConfig.ValueWP[0]);
		}else{
			sequence.initSequence(HConfig.ValueWP[1]);
		}
		driver = new Driver();
	}

	public void run(){
//		connection.start();
		connection.connectDB();
		
		while(true){
			synchronized(this){
				try {
					this.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if(Main.isActive == false){
				break;
			}
			while(this.checkDoSQLNow(0) > 0){
				for(int i = 0; i < base; i++){
					int sqlId = sequence.nextSequence();
					driver.initiatePara(sqlId);
//					connection.setPara(sqlId, driver.para, driver.paraType, driver.paraNumber, driver.PKNumber);
//					connection.doSQLNow ++;
					connection.doSQL(sqlId, driver.para, driver.paraType, driver.paraNumber, driver.PKNumber);
				}
				if(this.bonus > 0){
					int sqlId = sequence.nextSequence();
					driver.initiatePara(sqlId);
//					connection.setPara(sqlId, driver.para, driver.paraType, driver.paraNumber, driver.PKNumber);
//					connection.doSQLNow ++;
					connection.doSQL(sqlId, driver.para, driver.paraType, driver.paraNumber, driver.PKNumber);
					this.bonus --;
				}
				this.checkDoSQLNow(-1);
			}
		}
		try {
			connection.conn.close();
		} catch (SQLException e) {
		}
	}
	
	public synchronized int checkDoSQLNow(int action){
		if(action == 1){
			this.doSQLNow++;
		}else if(action == -1){
			this.doSQLNow --;
		}
		return this.doSQLNow;
	}
	
	public void setQT(int newqt){
		this.actualQT = newqt;
		base = this.actualQT / 20;
		bonus = this.actualQT % 20;
//		if(writeHeavy){
//			sequence.initSequence(HConfig.ValueWP[0]);
//		}else{
//			sequence.initSequence(HConfig.ValueWP[1]);
//		}
	}

}
