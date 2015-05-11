package hybridUtility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MConvert {
	public static String inPath = "/host/result/memory/memory.h500";
	public static String outPath = "/host/result/memory/memory.h500.unified";
	
	public static void main(String[] args) throws IOException{
		File in = new File(inPath);
		BufferedReader reader = new BufferedReader(new FileReader(in));
		File out = new File(outPath);
		BufferedWriter writer = new BufferedWriter(new FileWriter(out));
		String line;
		writer.write("#time dataM totalM javaM");
		while((line = reader.readLine()) != null){
			String[] splits = line.trim().split("\\s+");
			int time = Integer.parseInt(splits[0]);
			if(time < 300 || time > 2100){
				continue;
			}
			writer.write(""+(time-300));
			writer.write(" "+Double.parseDouble(splits[1])/1024.0);
			writer.write(" "+splits[2]);
			writer.write(" "+splits[3]);
//			writer.write(" "+Double.parseDouble(splits[4])/1024.0);
			writer.newLine();
		}
		reader.close();
		writer.close();
	}

}
