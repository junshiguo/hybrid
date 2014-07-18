package hybridController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;

public class SocketTask extends Thread {
	public int TYPE;
	public Socket socket;
	public BufferedReader reader;
	public Writer writer;
	
	public SocketTask(Socket s){
		this.socket =s;
	}

	@Override
	public void run() {
		try {
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
			writer = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");
			String str = null;
			while((str = reader.readLine()) != null){
				
			}
			//check if it is needed to write some info back
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setType(int t){
		this.TYPE = t;
	}

}
