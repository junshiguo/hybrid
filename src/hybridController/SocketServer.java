package hybridController;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class SocketServer {
	public static int port = 8899;
	public static ArrayList<SocketTask> task;
	public static boolean[] inPosition;
	public static boolean[] endTest;
	public static boolean ENDTEST = false;
	
	public static void main(String[] args) throws IOException{
		init();
		ServerSocket server = new ServerSocket(port);
		int count = 0;
		while(true){
			Socket socket = server.accept();
			SocketTask st = new SocketTask(socket);
			st.start();
			task.add(st);
			count ++;
			if(count == 18){
				break;
			}
		}
	}
	
	public static void init(){
		ENDTEST = false;
		task = new ArrayList<SocketTask>();
		inPosition = new boolean[18];
		endTest = new boolean[18];
		for(int i = 0; i < 18; i++){
			inPosition[i] = false;
			endTest[i] = false;
		}
	}
	
}
