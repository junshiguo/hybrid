package hybridUtility;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Vector;


public class CopyOfDataAnalyzer2 {
	public static int MinPerTest = 31;
	public static int TenantNumber = 3000;
	public static int CheckInterval = 5; //seconds
	public static String InputPath = "/host/result/ud_to_d/result_mysql";
	public static String OutputPath = "/host/result/ud_to_d";
	public static CopyOfDataAnalyzer2[] tenants = new CopyOfDataAnalyzer2[TenantNumber];
	public static int[] TotalVQPerMin = new int[MinPerTest];
	public static int[] TotalVTPerMin = new int[MinPerTest];
	public static Double AverageLatency;
	public static Double[] _latency = new Double[10];
	public static String fix = "m";
	public static boolean extraInterval = true;
	
	public static int[] CHECK_INTERVAL = {10};
	public static String[] PATH={"/host/result/5_50_500"};
	public static String[] PATH2 = {"a35", "a40"};
	public static String[] PATH3 = {/*"6", "7", "8", "9", "10"};//*/"1", "2", "3", "4", "5"};
	public static String[] FOLDER={"result_mysql", "result_hybrid_2000M"}; //, "result_hybrid_500M", "result_hybrid_1000M", "result_hybrid_1500M", "result_hybrid_2000M-2"};
	public static String[] FIX = {"m", "h2000"}; //"h500", "h1000", "h1500", "h2000-2"};	
	
	public static void main(String[] args){
		for(String path : PATH)
			for(String path2 : PATH2)
				for(String path3 : PATH3)
					for(int i = 0; i < FOLDER.length; i++){
						OutputPath = path + File.separator + path2 + File.separator + path3;
						InputPath = OutputPath + File.separator + FOLDER[i];
						for(int check : CHECK_INTERVAL){
							CheckInterval = check;
							fix = FIX[i]+"."+CheckInterval+"s";
//							try{
								DoAnalyzer();
								System.out.println();
//							}catch(Exception e){
//								System.out.println("Input: "+InputPath);
//								System.out.println("Output: "+OutputPath);
//								System.out.println("fix: "+fix+"\n**********************\n");
//							}
						}
					}
	}
	
	public static void DoAnalyzer(){
		//read files
		File folder = new File(InputPath);
		File[] files = folder.listFiles();
		for(File file: files){
			if(file.isFile()){
				CopyOfDataAnalyzer2 analyzer = new CopyOfDataAnalyzer2();
				analyzer.read(file);
				tenants[analyzer.id-1] = analyzer;
			}
		}
		//compute needed results
		init();
		for(int i = 0; i < TenantNumber; i++){
			for(int j = 0; j < MinPerTest; j++){
				int vq = tenants[i].VQPerMin[j];
				TotalVQPerMin[j] += vq;
				if(vq > 0)	TotalVTPerMin[j]++;
			}
		}
		Vector<Double> allLatencies = new Vector<Double>();
			for(int j = 0; j < TenantNumber; j++){
					allLatencies.addAll(tenants[j].latencies);
			}
			Collections.sort(allLatencies);
			int number = allLatencies.size();
			if(number == 0){
				for(int i = 0; i < 10; i++){
					_latency[i] = (double) 0;
				}
				AverageLatency = (double) 0;
			}else{
				for(int i = 0; i < 10; i++){
					_latency[i] = allLatencies.elementAt((int) ((50+i*5)/100.0*number));
				}
				double sum = 0;
				for(int j = 0; j < number; j++)
					sum += allLatencies.elementAt(j);
				AverageLatency = (sum/number);
			}
		//write results to file
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(OutputPath+File.separator+"_Latency."+fix));
			writer.write("# "+AverageLatency+"\n");
			for(int i = 0; i <10; i++){
				writer.write(""+(50+i*5)+" "+_latency[i]+"\n");
			}
			writer.close();
			
			writer = new BufferedWriter(new FileWriter(OutputPath+File.separator+"allLatency."+fix));
			int index = 0;
			for(Double d : allLatencies){
				writer.write(""+index+" "+d+"\n");
				index++;
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void init(){
		for(int i = 0; i < MinPerTest; i++){
			TotalVQPerMin[i] = 0;
			TotalVTPerMin[i] = 0;
		}
	}
	
	public int id, SLO, dataSize, writeHeavy;
	public int[] VQPerMin = new int[MinPerTest];
	public Vector<Double> latencies = new Vector<Double>();
	
	public void read(File file){
		BufferedReader reader = null;String line = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			
			String[] elements;
			reader.readLine();	reader.readLine();
			line = reader.readLine().trim();
			elements = line.split("\\s+");
			id = Integer.parseInt(elements[0].trim());
			SLO = Integer.parseInt(elements[1].trim());
			dataSize = Integer.parseInt(elements[2].trim());
			writeHeavy = Integer.parseInt(elements[3].trim());
			reader.readLine();
			
			reader.readLine(); reader.readLine();
			if(extraInterval){
				for(int i = 0; i < 5; i++){
					reader.readLine();
				}
			}
			
			for(int i = 0; i < MinPerTest; i++){
				line = reader.readLine().trim();
				elements = line.split("\\s+");
				VQPerMin[i] = Integer.parseInt(elements[3]);
			}
			reader.readLine();
			
			reader.readLine(); reader.readLine();
			double latency;
			while((line = reader.readLine()) != null){
				if(line.trim().startsWith("*")) break;
				elements = line.trim().split("\\s+");
				double endTime = Double.parseDouble(elements[4].trim());
				if(extraInterval && endTime < 5*60*1000) continue;
				latency = Double.parseDouble(elements[5].trim());
				latencies.add(latency);
			}
			reader.close();
		} catch (Exception e) {
			System.out.println("line: "+line);
			e.printStackTrace();
			
		}
	}
	
}
