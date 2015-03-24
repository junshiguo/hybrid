package hybridUtility;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Vector;


public class DataAnalyzer {
	public static int MinPerTest = 31;
	public static int TenantNumber = 3000;
	public static int CheckInterval = 10; //seconds
	public static String InputPath = "/host/result/result_new/results_new_1346_hybrid-2";
	public static String OutputPath = "/host/result/result_new";
	public static DataAnalyzer[] tenants = new DataAnalyzer[TenantNumber];
	public static int[] TotalVQPerMin = new int[MinPerTest];
	public static int[] TotalVTPerMin = new int[MinPerTest];
	public static Vector<Double> AverageLatency = new Vector<Double>();
	public static Vector<Double> _90Latency = new Vector<Double>();
	public static Vector<Double> _95Latency = new Vector<Double>();
	public static Vector<Double> _99Latency = new Vector<Double>();
	public static String fix = "h2000."+CheckInterval+"s";
	public static boolean extraInterval = true;
	
	public static int[] CHECK_INTERVAL = {10};
	public static String[] PATH={"/host/result/100_150_200/a30"};
//	public static String[] PATH2 = {"a30"};
	public static String[] PATH3 = {"1", "2", "3", "4", "5"};
	public static String[] FOLDER={"result_mysql", "result_hybrid_2000M"}; //"result_hybrid_500M", "result_hybrid_1000M", "result_hybrid_1500M", "result_hybrid_2000M-2"};
	public static String[] FIX = {"m", "h2000"}; // "h500", "h1000", "h1500", "h2000-2"};	
	public static boolean doConverge = true;
	
	public static void main(String[] args){
		if(doConverge){
			
		String path = "/host/result/100_150_200/a30";
		String folder[] = {"1", "2", "3", "4", "5"};
		String target[] = {"_Latency"};
		String fix[] =  {"m", "h2000"}; // "h2000-2", "h500", "h1000", "h1500"};
		int interval = 10;
		try {
			Converge(path, folder, target, fix, interval);
			ConvergeAllLatency(path, folder, fix);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		}else{
		
		for(String path : PATH)
//			for(String path2 : PATH2)
				for(String path3 : PATH3)
					for(int i = 0; i < FOLDER.length; i++){
//						OutputPath = path + File.separator + path2 + File.separator + path3;
						OutputPath = path + File.separator + path3;
						InputPath = OutputPath + File.separator + FOLDER[i];
						for(int check : CHECK_INTERVAL){
							CheckInterval = check;
							fix = FIX[i]+"."+CheckInterval+"s";
							init0();
//							try{
								DoAnalyzer();
//							}catch(Exception e){
//								System.out.println("Input: "+InputPath);
//								System.out.println("Output: "+OutputPath);
//								System.out.println("fix: "+fix+"\n**********************\n");
//							}
						}
					}
		}
	}
	
	public static void ConvergeAllLatency(String path, String[] folder, String[] fix) throws IOException{
		for(String fi : fix){
			String fileName = "allLatency."+fi+".10s";
			BufferedReader readers[] = new BufferedReader[folder.length];
			long totalCount = 0;
			Vector<Double> _allLatency = new Vector<Double>();
			for(int i = 0; i < folder.length; i++){
				totalCount += countRows(path+File.separator+folder[i]+File.separator+fileName);
				readers[i] = new BufferedReader(new FileReader(new File(path+File.separator+folder[i]+File.separator+fileName)));
			}
			long nextIndex = (long) (totalCount * 0.5);
			long nextStep = (long) (totalCount * 0.05);
			long nextCount = 0;
			String[] lines = new String[folder.length];
			String[][] splits = new String[folder.length][];
			Double[] values = new Double[folder.length];
//			Vector<Double> allLatency = new Vector<Double>();
			for (int i = 0; i < folder.length; i++) {
				lines[i] = readers[i].readLine();
				if (lines[i] == null)
					break;
				splits[i] = lines[i].split("\\s+");
				values[i] = Double.parseDouble(splits[i][1]);
			}
			nextCount = 5;
			while(true){
				int minIndex = findMinIndex(values);
				nextCount++;
				if(nextCount == nextIndex){
					_allLatency.add(values[minIndex]);
					if(_allLatency.size() == 10)
						break;
					nextIndex += nextStep;
				}
//				allLatency.add(values[minIndex]);
				lines[minIndex] = readers[minIndex].readLine();
				if(lines[minIndex] != null){
					splits[minIndex] = lines[minIndex].split("\\s+");
					values[minIndex] = Double.parseDouble(splits[minIndex][1]);
				}else{
					values[minIndex] = -1.;
				}
				boolean cont = false;
				for(Double d : values){
					if(d >= 0){
						cont = true;
						break;
					}
				}
				if(cont == false){
					break;
				}
			}
			for(BufferedReader reader : readers){
				reader.close();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path+File.separator+fileName)));
			for(int i = 0; i < 10; i++){
				writer.write(""+(50+i*5)+" "+_allLatency.elementAt(i));
				writer.newLine();
			}
			writer.close();
		}
	}
	
	public static long countRows(String file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(new File(file)));
		int count = 0;
		while(reader.readLine() != null)
			count++;
		reader.close();
		return count;
	}
	
	public static int findMinIndex(Double[] values){
		if(values == null || values.length < 1)
			return -1;
		int ret = 0;
		Double min = values[0];
		for(int i = 1; i < values.length; i++){
			if(min < 0 || (values[i] >= 0 && min > values[i])){
				ret = i;
				min = values[i];
			}
		}
		return ret;
	}
	//path="/host/result/5_50_500/a30"; folder={"3","4","5","6","7"};target={"Violation","Latency","Latency-3d"};
	//fix={"m","h500","h1000","h1500","h2000"};interval=10;
	public static void Converge(String path, String[] folder, String[] target, String[] fix, int interval) throws IOException{
		for(String ta : target){
			for(String fi : fix){
				String fileName = ta+"."+fi+"."+interval+"s";
				BufferedReader readers[] = new BufferedReader[folder.length];
				for(int i = 0; i < folder.length; i++){
					readers[i] = new BufferedReader(new FileReader(new File(path+File.separator+folder[i]+File.separator+fileName)));
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path+File.separator+fileName)));
				String[] lines = new String[folder.length];
				String[][] splits = new String[folder.length][];
				Double dd;
				while(true){
					for(int i = 0; i < folder.length; i++){
						lines[i] = readers[i].readLine();
					}
					if(lines[0] == null){
						break;
					}
					if(lines[0].trim().equals("")){
						continue;
					}
					for(int i = 0; i < folder.length; i++){
						splits[i] = lines[i].split("\\s+");
					}
					if(ta == "Violation"){
						writer.write(splits[0][0]);
						for(int i = 1; i < splits[0].length; i++){
							dd = (double) 0;
							for(int j = 0; j < folder.length; j++){
								dd += Double.parseDouble(splits[j][i]);
							}
							writer.write(" "+(double)(dd*1.0/folder.length));
						}
						writer.newLine();
					}else if(ta == "Latency"){
						if(Integer.parseInt(splits[0][0]) > 1800){
							continue;
						}
						writer.write(splits[0][0]);
						int avgLength = folder.length;
						for(int i = 1; i < splits[0].length-1; i++){
							dd = (double) 0;
							for(int j = 0; j < folder.length; j++){
								if(Integer.parseInt(splits[j][11]) > 5000 || Integer.parseInt(splits[j][11]) == 0){
									dd += Double.parseDouble(splits[j][i]);
								}else{
									avgLength --;
								}
							}
							if(avgLength != 0){
								writer.write(" "+(double)(dd*1.0/avgLength));
							}else{
								writer.write(" 0");
							}
						}
						writer.newLine();
					}else if (ta.equals("_Latency")){
						if(splits[0][0].equals("#"))
							continue;
						writer.write(splits[0][0]);
						for(int i = 1; i < splits[0].length; i++){
							dd = (double) 0;
							for(int j = 0; j < folder.length; j++){
								dd += Double.parseDouble(splits[j][i]);
							}
							writer.write(" "+(double)(dd*1.0/folder.length));
						}
						writer.newLine();
					}else{
						if(Integer.parseInt(splits[0][0]) > 1800 || Integer.parseInt(splits[0][1]) > 90)
							continue;
						writer.write(splits[0][0]+" "+splits[0][1]);
						dd = 0.0;
						int avgLength = folder.length;
						for(int j = 0; j < folder.length; j++){
							if(Integer.parseInt(splits[j][3]) > 5000 || Integer.parseInt(splits[j][3]) == 0){
								dd += Double.parseDouble(splits[j][2]);
							}else{
								avgLength --;
							}
						}
						if(avgLength != 0){
							writer.write(" "+dd/avgLength);
						}else{
							writer.write(" 0");
						}
						writer.newLine();
						if(Integer.parseInt(splits[0][1]) == 90) writer.newLine();
					}
				}
				writer.close();
				for(BufferedReader reader : readers){
					reader.close();
				}
			}
		}
	}
	
	public static Vector<Vector<Double>> ALL_LATENCY = new Vector<Vector<Double>>();
	public static void DoAnalyzer(){
		//read files
		File folder = new File(InputPath);
		File[] files = folder.listFiles();
		for(File file: files){
			if(file.isFile()){
				DataAnalyzer analyzer = new DataAnalyzer();
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
		Vector<Double> allLatencies;
		ALL_LATENCY = new Vector<Vector<Double>>();
		int totalCheck = MinPerTest*60/CheckInterval;
		for(int i = 0; i < totalCheck; i++){
			allLatencies = new Vector<Double>();
			for(int j = 0; j < TenantNumber; j++){
				if(tenants[j].latencies.size() > i){
					allLatencies.addAll(tenants[j].latencies.elementAt(i));
				}
			}
			Collections.sort(allLatencies);
			ALL_LATENCY.add(allLatencies);
		}
		//write results to file
		try {
//			BufferedWriter writer = new BufferedWriter(new FileWriter(OutputPath+File.separator+"Violation."+fix));
//			for(int i = 0; i < MinPerTest; i++){
//				writer.write(i+" "+TotalVQPerMin[i]+" "+TotalVTPerMin[i]+"\n");
//			}
//			writer.close();
//			
//			writer = new BufferedWriter(new FileWriter(OutputPath+File.separator+"Latency."+fix));
//			for(int i = 0; i < ALL_LATENCY.size(); i++){
//				writer.write(""+i*CheckInterval);
//				allLatencies = ALL_LATENCY.elementAt(i);
//				int number = allLatencies.size();
//				if(allLatencies.isEmpty()){
//					for(int j = 1; j <= 10; j++){
//						writer.write(" 0");
//					}
//				}else{
//					for(int j = 1; j <= 10; j++){
//						int index = (int) (j*10/100.0*number);
//						index = (index >= number)?number-1:index;
//						index = (index < 0)?0:index;
//						writer.write(" "+allLatencies.elementAt(index));
//					}
//				}
//				writer.write(" "+number);
//				writer.newLine();
//			}
//			writer.close();
//			
//			 writer = new BufferedWriter(new FileWriter(OutputPath+File.separator+"Latency-3d."+fix));
//			for(int i = 0; i < ALL_LATENCY.size(); i++){
//				allLatencies = ALL_LATENCY.elementAt(i);
//				int number = allLatencies.size();
//				if(allLatencies.isEmpty()){
//					for(int j = 0; j <=19; j++){
//						writer.write(""+i*CheckInterval+" "+j*5+" 0 0\n");
//					}
//				}else{
//					Collections.sort(allLatencies);
//					for(int j = 0; j <= 19; j++){
//						int index = (int) (j*5/100.0*number);
//						index = (index >= number)?number-1:index;
//						index = (index < 0)?0:index;
//						writer.write(""+i*CheckInterval+" "+j*5+" "+allLatencies.elementAt(index)+" "+number+"\n");
//					}
//				}
//				writer.newLine();;
//				writer.flush();
//			}
//			writer.close();
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(OutputPath+File.separator+"allLatency-a."+fix));
			Vector<Double> AllLatency = new Vector<Double>();
			for(Vector<Double> al : ALL_LATENCY){
				if(al.isEmpty() == false && al.size() > 5000){
					AllLatency.addAll(al);
				}
			}
			Collections.sort(AllLatency);
			int index = 0;
			for(Double d : AllLatency){
				writer.write(""+index+" "+d);
				writer.newLine();
				index++;
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void init0(){
		tenants = new DataAnalyzer[TenantNumber];
		TotalVQPerMin = new int[MinPerTest];
		TotalVTPerMin = new int[MinPerTest];
		AverageLatency = new Vector<Double>();
		_90Latency = new Vector<Double>();
		_95Latency = new Vector<Double>();
		 _99Latency = new Vector<Double>();
	}
	
	public static void init(){
		for(int i = 0; i < MinPerTest; i++){
			TotalVQPerMin[i] = 0;
			TotalVTPerMin[i] = 0;
		}
	}
	
	public int id, SLO, dataSize, writeHeavy;
	public int[] VQPerMin = new int[MinPerTest];
	public Vector<Vector<Double>> latencies = new Vector<Vector<Double>>();
	
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
//				elements = line.split("\\s+");
//				VQPerMin[i] = Integer.parseInt(elements[2]) - Integer.parseInt(elements[4]);
//				if(VQPerMin[i] < 0) VQPerMin[i] = 0;
			}
			reader.readLine();
			
			reader.readLine(); reader.readLine();
			int index = 0;
			double endTime, latency;
			Vector<Double> tmpLatency = new Vector<Double>();
			while((line = reader.readLine()) != null){
				if(line.startsWith("*")) break;
				elements = line.trim().split("\\s+");
				endTime = Double.parseDouble(elements[4].trim());
				if(extraInterval && endTime < 5*60*1000) continue;
				latency = Double.parseDouble(elements[5].trim());
				while(true){ //
					if((extraInterval && endTime > index*CheckInterval*1000 + 5*60*1000) || (extraInterval == false && endTime > index*CheckInterval*1000)){
						latencies.add(tmpLatency);
						tmpLatency = new Vector<Double>();
						index++;
					}else{
						break;
					}
					
				}
				tmpLatency.add(latency);
			}
			reader.close();
		} catch (Exception e) {
			System.out.println(file.getName());
			System.out.println("line: "+line);
			e.printStackTrace();
			
		}
	}
	
}
