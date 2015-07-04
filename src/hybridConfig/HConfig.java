package hybridConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class HConfig {
	public static int VoltdbVolumn = 1024;
	public static int totalTenant = 3000;
	
	public static int[] ValueQT = { 5, 50, 500 };  // per minute
	public static int[] ValueDS = {7, 16, 35 };  // MB
	public static double[] ValueWP = { 0.6, 0.4 };  //
	public static double[] PercentQT = { 0.3, 0.5, 0.2 };
	public static int[] QTPer1000 = {300, 500, 200};
	public static double[] PercentDS = { 0.5, 0.3, 0.2 };
	public static double[] PercentWH = { 0.4, 0.6 };  // 0.4 for write heavy and 0.6 for read heavy
	public static double[]  PercentTenantSplits  = {
		0.06	//QT: 20, DS: 3, write heavy
		,0.09	//QT: 20, DS: 3, read heavy
		,0.1	//QT: 60, DS: 3, write heavy
		,0.15	//QT: 60, DS: 3, read heavy
		,0.04	//QT: 100, DS: 3, write heavy
		,0.06	//QT: 100, DS: 3, read heavy
		
		,0.036		//QT: 20, DS: 6.9, write heavy
		,0.054	//QT: 20, DS: 6.9, read heavy
		,0.06	//QT: 60, DS: 6.9, write heavy
		,0.09	//QT: 60, DS: 6.9, read heavy
		,0.024	//QT: 100, DS: 6.9, write heavy
		,0.036	//QT: 100, DS: 6.9, read heavy
		
		,0.024	//QT: 20, DS: 18.3, write heavy
		,0.036	//QT: 20, DS: 18.3, read heavy
		,0.04	//QT: 60, DS: 18.3, write heavy
		,0.06	//QT: 60, DS: 18.3, read heavy
		,0.016	//QT: 100, DS: 18.3, write heavy
		,0.024	//QT: 100, DS: 18.3, read heavy
	};
	public static int TenantPerType1000[] = {
		60, 90, 100, 150, 40, 60,
		36, 54, 60, 90, 24, 36,
		24, 36, 40, 60, 16, 24
	};
	public static double[] PercentTenantSplitsSum;
	public static int[] TenantIdRange;
	public static int QTMatrix[] = {ValueQT[0],ValueQT[0],ValueQT[1],ValueQT[1],ValueQT[2],ValueQT[2],
		ValueQT[0],ValueQT[0],ValueQT[1],ValueQT[1],ValueQT[2],ValueQT[2],
		ValueQT[0],ValueQT[0],ValueQT[1],ValueQT[1],ValueQT[2],ValueQT[2]};
	public static int DSMatrix[] = {ValueDS[0],ValueDS[0],ValueDS[0],ValueDS[0],ValueDS[0],ValueDS[0],
		ValueDS[1],ValueDS[1],ValueDS[1],ValueDS[1],ValueDS[1],ValueDS[1],
		ValueDS[2],ValueDS[2],ValueDS[2],ValueDS[2],ValueDS[2],ValueDS[2]};
	public static boolean WHMatrix[] = {true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false};
	
	public static void init(int n){
		totalTenant = n;
		TenantIdRange = new int[18];
		PercentTenantSplitsSum = new double[18];
		for(int i = 0; i < 18; i++){
			if(i == 0) PercentTenantSplitsSum[i] = PercentTenantSplits[0];
			else	PercentTenantSplitsSum[i] = PercentTenantSplitsSum[i-1]+PercentTenantSplits[i];
			TenantIdRange[i] = (int) (totalTenant*PercentTenantSplitsSum[i]);
		}
	}
	public static void main(String[] args){
		init(3000);
		try {
			writeInfo();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void writeInfo() throws IOException{
		FileWriter fstream = null;
		fstream = new FileWriter("tenants_info", false);
		BufferedWriter out = new BufferedWriter(fstream);
		for(int t = 0; t < 18; t++){
			String str = "";
			if(t%6 == 0 || t%6 == 1)	str += " 20";
			else if(t%6 == 2 || t%6 == 3) str += " 60";
			else str += " 200";
			if(t/6 == 0) str += " 7";
			else if(t/6 == 1) str += " 16";
			else str += " 35";
			if(t%2 == 0) str += " 60";
			else str += " 40";
			int n = (int) (PercentTenantSplits[t]*totalTenant);
			int start = getStartId(t);
			for(int j = 0; j < n; j++){
				out.write((start+1+j)+str+"\n");
			}
		}
		out.close();
	}
	/**
	 * 	TYPE info according PercentTenantSplits
	 * @param tenantId
	 * @return
	 */
	public static int getType(int tenantId){
		for(int i = 0; i < 18; i++){
			if(TenantIdRange[i] > tenantId){
				return i;
			}
		}
		return 17;
	}
	/**
	 * 	use getQT(tenantId, false) to get SLO with tenant id
	 * @param tenantId
	 * @param isType, true if tenantId represents TYPE; false if tenantId represents tenant id
	 * @return
	 */
	public static int getQT(int tenantId, boolean isType){
		int type = tenantId;
		if(isType == false){
			type = getType(tenantId);
		}
		return HConfig.QTMatrix[type];
	}
	
	public static int getDS(int tenantId, boolean isType){
		int type = tenantId;
		if(isType == false){
			type = getType(tenantId);
		}
		return DSMatrix[type];
	}
	
	public static int getDSType(int tenantId, boolean isType){
		int type = tenantId;
		if(isType == false){
			type = getType(tenantId);
		}
		if(type < 6){
			return 0;
		}else if(type < 12){
			return 1;
		}else{
			return 2;
		}
	}
	
	public static boolean isWriteHeavy(int tenantId, boolean isType){
		int type = tenantId;
		if(isType == false){
			type = getType(tenantId);
		}
		return WHMatrix[type];
	}
	
	public static int getStartId(int type){
		if(type == 0) return 0;
		return TenantIdRange[type-1];
	}
	
}
