package hybridController;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer extends Thread{
	
	public void run(){
		ServerSocket server;
		try {
			server = new ServerSocket(HybridController.port);
			while(true){
				Socket socket = server.accept();
				SocketTask st = new SocketTask(socket);
				st.start();
				HybridController.task.add(st);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
