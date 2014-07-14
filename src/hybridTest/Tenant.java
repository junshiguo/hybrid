package hybridTest;

public class Tenant extends Thread {
	public int tenantId;
	public double dataSize;
	public int QT;
	public int actualQT;  //actual throughput per minute
	public boolean writeHeavy;
	
	public String dbURL;
	public String dbUsername;
	public String dbPassword;
	public String voltdbServer;
	public int connNumber;
	public int requestNumber; //requests per minute
	public HConnection[] connections;
	public HSequence sequence;
	public Driver driver;
	
	public Tenant(int id, double ds, int qt, boolean wh,  String url, String username, String password, String voltdbServer){
		this.tenantId = id;
		this.dataSize = ds;
		this.QT = qt; 
		this.writeHeavy = wh;
		this.dbURL = url;
		this.dbUsername = username;
		this.dbPassword = password;
		this.voltdbServer = voltdbServer;
		connNumber = QT/20;
		requestNumber = connNumber;
		connections = new HConnection[connNumber];
		for(int i = 0; i < connNumber; i++){
			connections[i] = new HConnection(i, id, url, username, password, voltdbServer);
		}
		if(writeHeavy){
			sequence = new HSequence(Main.ValueWP[0], requestNumber);
		}else{
			sequence = new HSequence(Main.ValueWP[1], requestNumber);
		}
		driver = new Driver();
	}

	public void run(){
		for(int i = 0; i < connNumber; i++){
			connections[i].start();
		}
		while(Main.isActive == false){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		while(Main.isActive){
			for(int i = 0; i < requestNumber; i++){
				int sqlId = sequence.next();
				driver.initiatePara(sqlId);
				connections[i].setPara(sqlId, driver.para, driver.paraType, driver.paraNumber);
				connections[i].doSQLNow = true;
			}
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void setQT(int newqt){
		this.actualQT = newqt;
		requestNumber = actualQT/20;
		if(writeHeavy){
			sequence = new HSequence(Main.ValueWP[0], requestNumber);
		}else{
			sequence = new HSequence(Main.ValueWP[1], requestNumber);
		}
	}

}
