package hybridTest;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.ArrayList;

public class SocketSender extends Thread {
	public Socket socket;
	public Writer writer;
	
	
	public int sendNow = 0;
	public ArrayList<Integer> infoType = new ArrayList<Integer>();
	public ArrayList<String> info = new ArrayList<String>();
	
	public void run(){
		try {
			socket = new Socket(Main.SocketServer, Main.port);
			writer = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");
			writer.write(Main.TYPE+"&sender\n");
			writer.flush();
			while(true){
				if(this.sendNow > 0){
					int tmp = infoType.get(0);
					infoType.remove(0);
					String str = info.get(0);
					info.remove(0);
					writer.write(Main.TYPE + "&" + tmp + "&" + str + "\n");
					writer.flush();
					this.sendNow--;
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public void sendInfo(long time, int[] tp, int[] atp){
		this.infoType.add(2);
		int lateTenant = 0, lateQuery = 0;
		for(int i = 0; i < Main.tenantNumber; i++){
			tp[i] = tp[i] * 60 / Main.checkTp;
			if(atp[i]<tp[i]){
				lateTenant ++;
				lateQuery += tp[i] - atp[i];
			}
		}
		this.info.add(time+" "+lateTenant+" "+lateQuery);
		this.sendNow++;
	}
	
	public void sendInfo(long time, long throughput, double wp){
		this.infoType.add(3);
		this.info.add(time+" "+throughput+" "+wp);
		this.sendNow++;
	}
	
	public void sendInfo(String str){
		this.infoType.add(0);
		this.info.add(str);
		this.sendNow++;
	}

}
