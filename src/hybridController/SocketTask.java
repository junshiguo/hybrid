package hybridController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.ArrayList;

public class SocketTask extends Thread {
	public int TYPE;
	public boolean isSender = false;
	public Socket socket;
	public BufferedReader reader;
	public Writer writer;
	
	public int sendNow = 0;
	public ArrayList<Integer> infoType;
	public ArrayList<String> stringInfo;
	
	public SocketTask(Socket s){
		this.socket =s;
		infoType = new ArrayList<Integer>();
		stringInfo = new ArrayList<String>();
		sendNow = 0;
	}

	@Override
	public void run() {
		try {
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
			writer = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");
			String str = null;
			String[] info = null;
			str = reader.readLine();
			if(str != null)	info = str.trim().split("&");
			this.setType(Integer.parseInt(info[0]));
			
			if(info[1].equals("sender")){
				this.isSender = false;
				while((str = reader.readLine()) != null){
					info = str.trim().split("&");
					switch(Integer.parseInt(info[1])){
					case 0: 
						if(info[2].trim().equals("in position")){
							System.out.println("server received: Main "+TYPE);
							HybridController.launchTask(this, TYPE, isSender); // lauch task
							
						}else if(info[2].trim().equals("end test")){
							HybridController.sendTask[TYPE].sendInfo("end test");
							System.out.println("server received: Main "+TYPE+" end test!");
						}
						break;
					case 2:
						String[] message = info[2].split(" ");
						HybridController.lateTenant[Integer.parseInt(message[0].trim())] += Integer.parseInt(message[1].trim());
						HybridController.lateQuery[Integer.parseInt(message[0].trim())] += Integer.parseInt(message[2].trim());
						System.out.println("server received: Main:"+TYPE+" "+info[2]);
						break;
					case 3:
						//not sending throughput info now
						System.out.println("server received: "+str);
						default:
					}
				}
				socket.close();
				HybridController.inPosition[TYPE] = false;
			}else{
				this.isSender = true;
				HybridController.launchTask(this, TYPE, isSender);
//				System.out.println("server: TYPE "+TYPE+" send task working...");
				while(true){
					synchronized(this){
						this.wait();
					}
					if(this.checkSendNow(0) > 0){
						int tmp = this.infoType.get(0);
						this.infoType.remove(0);
						writer.write(tmp+"&"+stringInfo.get(0)+"\n");
						writer.flush();
						System.out.println("server sent: Main "+TYPE+", "+tmp+" "+stringInfo.get(0));
						stringInfo.remove(0);
						this.checkSendNow(-1);
					}
				}
			}
		} catch (IOException | InterruptedException e) {
			HybridController.senderInPosition[TYPE] = false;
			HybridController.inPosition[TYPE] = false;
//			e.printStackTrace();
		}
	}
	
	public synchronized int checkSendNow(int action){
		if(action == 1){
			this.sendNow++;
		}else if(action == -1){
			this.sendNow --;
		}
		return this.sendNow;
	}
	
	public void setType(int t){
		this.TYPE = t;
	}
	
	public void sendInfo(String info){
		this.infoType.add(0);
		this.stringInfo.add(info);
		this.checkSendNow(1);
		synchronized(this){
			this.notify();
		}
	}
	
	public void sendInfo(int tenantId, int isUsingV, int isPartiallyUsingV, int volumnId){
		this.infoType.add(1);
		this.stringInfo.add(""+tenantId+" "+isUsingV+" "+isPartiallyUsingV+" "+volumnId);
		this.checkSendNow(1);
		synchronized(this){
			this.notify();
		}
	}

}
