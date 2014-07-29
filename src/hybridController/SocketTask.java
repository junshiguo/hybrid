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
					switch(Integer.parseInt(info[2])){
					case 0: 
						HybridController.receiveTask[TYPE] = this;
						HybridController.inPosition[TYPE] = true;
						boolean flag = true;
						for(int i = 0; i < HybridController.typeNumber; i++){
							if(HybridController.inPosition[i] == false){
								flag = false;
							}
						}
						if(true){
							HybridController.sendTask[TYPE].sendInfo("all in position");
						}
						break;
					case 2:
						String[] message = info[2].split(" ");
						HybridController.lateTenant[Integer.parseInt(message[0].trim())] += Integer.parseInt(message[1].trim());
						HybridController.lateQuery[Integer.parseInt(message[0].trim())] += Integer.parseInt(message[2].trim());
						break;
					case 3:
						//not sending throughput info now
						default:
					}
				}
			}else if(info[1].equals("receiver")){
				this.isSender = true;
				while(true){
					if(this.sendNow > 0){
						int tmp = this.infoType.get(0);
						this.infoType.remove(0);
						writer.write(tmp+"&"+stringInfo.get(0));
						stringInfo.remove(0);
						this.sendNow--;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setType(int t){
		this.TYPE = t;
	}
	
	public void sendInfo(String info){
		this.infoType.add(0);
		this.stringInfo.add(info);
		this.sendNow++;
	}
	
	public void sendInfo(int tenantId, int isUsingV, int isPartiallyUsingV){
		this.infoType.add(1);
		this.stringInfo.add(""+tenantId+" "+isUsingV+" "+isPartiallyUsingV);
		this.sendNow++;
	}

}
