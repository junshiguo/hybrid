package hybridTest;

public class Tenant extends Thread {
	public int tenantId;
	public double dataSize;
	public int QT;
	public boolean writeHeavy;
	
	public String dbURL;
	public String dbUsername;
	public String dbPassword;
	public String voltdbServer;
	public TConnection[] connections;
	
	public Tenant(int id, double ds, int qt, boolean wh,  String url, String username, String password, String voltdbServer){
		this.tenantId = id;
		this.dataSize = ds;
		this.QT = qt; 
		this.writeHeavy = wh;
		this.dbURL = url;
		this.dbUsername = username;
		this.dbPassword = password;
		this.voltdbServer = voltdbServer;
		int connNumber = QT/20;
		connections = new TConnection[connNumber];
		for(int i = 0; i < connNumber; i++){
			connections[i] = new TConnection(id, url, username, password, voltdbServer);
		}
	}
	
	

}
