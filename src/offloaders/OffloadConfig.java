package offloaders;

public class OffloadConfig {
	public static String url = "jdbc:mysql://127.0.0.1:3306/test";
	public static String username = "remote", password = "remote";
	public static String voltdbServer = "127.0.0.1";
	public static String csvPath = "/tmp/hybrid";
	public static int batch = 200;
	
	public static void configure(String url, String un, String pw, String vServer){
		OffloadConfig.url = url;
		OffloadConfig.username = un;
		OffloadConfig.password = pw;
		OffloadConfig.voltdbServer = vServer;
	}
	
	public static void setCsvPath(String path){
		csvPath = path;
	}
	
}
