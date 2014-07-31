package hybridTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;

public class StateReceiver extends Thread {
	public Socket socket;
	public Writer writer;
	public BufferedReader reader;
	public boolean alive = true;
	
	@Override
	public void run() {
		try {
			socket = new Socket(Main.SocketServer, Main.port);
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
			writer = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");
			writer.write(Main.TYPE+"&receiver\n");
			writer.flush();
			System.out.println("Main "+Main.TYPE+" receive socket working...");
			String str = null;
			String[] info;
			while(this.alive && (str = reader.readLine()) != null){
				info = str.trim().split("&");
				if(info[0].trim().equals("0")){
					if(info[1].trim().equals("all in position")){
						Main.checkStart(true);
						System.out.println("Main: start test");
					}else{
						alive = false;
					}
				}else{
					String[] message = info[1].split(" ");
					int tenantId = Integer.parseInt(message[0].trim());
					int usingV = Integer.parseInt(message[1].trim());
					int usingPV = Integer.parseInt(message[2].trim());
					Main.setDBState(tenantId, usingV, usingPV);
				}
			}
		} catch (IOException e1) {
		}
	}

}
