package hybridUtility;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import utility.DBManager;
import utility.Sequence;

/**
 * 
 * @author guojunshi
 *
 */
public class MysqlWarmUp extends Thread {
	public static boolean isActive = false;
	public static int threadNumber = 100;
	public static int tenantPerThread = 30;
	public static MysqlWarmUp[] warmupThread;
	
	public static void main(String[] args){
		isActive = true;
		warmupThread = new MysqlWarmUp[threadNumber];
		String dbname = "tpcc3000";
		if(args.length > 0){
			dbname = args[0].trim();
			if(dbname.equals("tpcc3000") || dbname.equals("tpccM3000"))	tenantPerThread = 30;
			else if(dbname.equals("tpccM1500"))	tenantPerThread = 15;
		}
		for(int i = 0; i < threadNumber; i++){
			warmupThread[i] = new MysqlWarmUp(i, "jdbc:mysql://10.20.2.211/"+dbname, "remote", "remote");
			warmupThread[i].start();
		}
		try {
			for(int i = 0; i < 180; i++){
				Thread.sleep(5*1000);
				System.out.print(" . ");
				if(i % 12 == 11){
					System.out.println();
				}
			}
			isActive = false;
			Thread.sleep(10*1000);
			System.exit(0);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public int id;
	public Connection connection;
	public String url, username, password;
	public Statement stmt;
	public Sequence sequence;
	
	public MysqlWarmUp(int id, String url, String username, String password){
		this.id = id;
		this.url = url;
		this.username = username;
		this.password = password;
		sequence = new Sequence();
		int tmp = new Random(System.nanoTime()).nextInt(100);
		tmp = (tmp > 20)?(tmp - 20): 0;
		sequence.initSequence(100-tmp, tmp);
	}

	public void run(){
		connection = DBManager.connectDB(url, username, password);
		System.out.println("thread "+id+" db connected...");
		try {
			stmt = connection.createStatement();
			connection.setAutoCommit(false);
			new Driver(id);
			connection.commit();
			stmt.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	

}
