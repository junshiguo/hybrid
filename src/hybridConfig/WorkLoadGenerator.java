package hybridConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Random;

import utility.Support;

public class WorkLoadGenerator {
	public static double activeRatio = 0.30;
	public static double exchangeRatio = 0.1;
	public static int totalTenant = 3000;
	public static int timePerInterval = 5; //min
	public static int totalInterval = 7; // 30 min
	public static int HRan = 120;
	public static int MRan = 10;
	public static int MRan2 = 10;
	public static int LRan = 0;
	public static int LRan2 = -50;
	public static int dRan = 0;
	public static boolean[][] activePattern = new boolean[totalTenant][totalInterval];
	public static boolean[] isBursty = new boolean[totalInterval];
	
	public static int activeNumber;
	public static int[] activeTenant;
	public static int[] inactiveTenant;
	
	public static double[] percentActive = {0.3, 0.5, 0.2};
	public static int[] at0, at1, at2;
	public static int[] inat0, inat1, inat2;
	
	public static boolean FULL_WORKLOAD = false;
	public static int actualTenant = 1000;
	public static double bias = 0.;
	public static double throt = 0.8;
	
	public static void main(String[] args) throws IOException{
		totalTenant = 2000;
		activeRatio = 0.25;
		exchangeRatio = 0.1;
		if(args.length > 0){
			String[] value = args[0].trim().split("_");
			if(value.length < 3){
				System.out.println("SLO vector wrong. Using 5_50_500.");
				value = "5_50_500".split("_");
			}
			for(int i = 0; i < 3; i++){
				HConfig.ValueQT[i] = Integer.parseInt(value[i]);
			}
		}
		if(args.length > 1){
			totalTenant = Integer.parseInt(args[1]);
		}
		if(args.length > 2){
			activeRatio = Double.parseDouble(args[2]);
		}
		if(args.length > 3){
			exchangeRatio = Double.parseDouble(args[3]);
		}
		if(args.length > 4){
			throt = Double.parseDouble(args[4]);
		}
		activeNumber = (int) (activeRatio * totalTenant);
		activeTenant = new int[activeNumber];
		inactiveTenant = new int[totalTenant - activeNumber];
		HConfig.init(totalTenant);
		generateLoad1();
	}
//	public static void main(String[] args){
//		totalTenant = 3000;
//		HConfig.init(totalTenant);
//		for(int i = 0; i < 10; i++){
//			System.out.print(HConfig.getQT(getId(i), false)+" ");
//			if(i%50 == 0){
//				System.out.println();
//			}
//		}
//	}
	
	//start from 0
	public static int getId(int tenantId){
		if(totalTenant % 1000 == 0){
			int multi = totalTenant / 1000;
			int sum = 0;
			for(int i = 0; i < 18; i++){
				if(sum + HConfig.TenantPerType1000[i] * multi > tenantId){
					if(i == 0)	return tenantId;
					return HConfig.TenantIdRange[i-1]+tenantId - sum;
				}else{
					sum += HConfig.TenantPerType1000[i] * multi;
				}
			}
		}else{
			int sum = 0;
			for(int i = 0; i < 18; i++){
				if(sum + totalTenant * HConfig.PercentTenantSplits[i] > tenantId){
					return HConfig.TenantIdRange[i]+tenantId - sum;
				}else{
					sum += totalTenant * HConfig.PercentTenantSplits[i];
				}
			}
		}
		return -1;
	}
	
	/**
	 * each minute is a line of workload
	 * @throws IOException
	 */
	public static void generateLoad1() throws IOException{
		setBursty();
		setActivePattern(percentActive);
		FileWriter fstream = null;
		fstream = new FileWriter("load.txt", false);
		BufferedWriter out = new BufferedWriter(fstream);
		DecimalFormat df = new DecimalFormat("0.00");
		out.write(""+totalTenant+" "+(totalInterval+1)+" "+df.format(activeRatio)+" "+df.format(exchangeRatio));
		out.newLine(); out.flush();
		for(int i = 0; i < totalTenant; i++){
			out.write(""+(getId(i)+1)+" ");
		}
		out.newLine(); out.flush();
		for(int i = 0; i < totalTenant; i++){
			out.write(""+HConfig.getQT(getId(i), false)+" ");
		}
		out.newLine(); out.flush();
		for(int i = 0; i < totalTenant; i++){
			out.write(""+HConfig.getDS(getId(i), false)+" ");
		}
		out.newLine(); out.flush();
		
		//interval 0 repeat
		out.write(""+activeNumber);
		for(int i = 0; i < totalTenant; i++){
			if(activePattern[i][0] == true){
				out.write(" "+(getId(i)+1));
			}
		}
		out.write("\n");
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			out.write(""+activeNumber);
			for(int i = 0; i < totalTenant; i++){
				if(activePattern[i][intervalId] == true){
					out.write(" "+(getId(i)+1));
				}
			}
			out.write("\n");
		}
//		out.write("\n");
		
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			
			int[] load = new int[totalTenant];
			Random ran = new Random(System.nanoTime());
			for (int tenantId = 0; tenantId < totalTenant; tenantId++) {
				int QT = HConfig.getQT(getId(tenantId), false);
				boolean isActive = activePattern[tenantId][intervalId];
				if (isActive) {
					if(FULL_WORKLOAD == true){
						load[tenantId] = QT;
					}else{
						if(isBursty[intervalId]){
							load[tenantId] = (int) (PossionDistribution.getRandomNumber((int) ((QT)*throt)));
//							if(ran.nextBoolean()){
//								load[tenantId] = (int) (PossionDistribution.getRandomNumber((int) (QT)) * (1+bias));
//							}else{
//								load[tenantId] = (int) (PossionDistribution.getRandomNumber((int) (QT)) * (1-bias));
//							}
						}else{
							load[tenantId] = PossionDistribution.getRandomNumber(QT/10);
						}
					}
				} else {
					load[tenantId] = 0;
				}
			}
//			for (int time = 0; time < timePerInterval; time++) {
//				out.write("" + (intervalId * timePerInterval + time));
//				int dd = ran.nextInt() % dRan;
			if(intervalId == 0){
				for(int min = 0; min < timePerInterval; min++){
					out.write(""+(intervalId*timePerInterval + min+1));
					int totalLoad = 0;
					for (int tenantId = 0; tenantId < totalTenant; tenantId++) {
						boolean isActive = activePattern[tenantId][intervalId];
						int tmpload = 0;
						if (isActive) {
							tmpload = load[tenantId];
						}
						out.write(" " + tmpload);
						totalLoad += tmpload;
					}
					out.write(" " + totalLoad);
					out.newLine();out.flush();
				}
			}
			for(int min = 0; min < timePerInterval; min++){
			out.write(""+(intervalId*timePerInterval + min+6));
				int totalLoad = 0;
				for (int tenantId = 0; tenantId < totalTenant; tenantId++) {
					int QT = HConfig.getQT(tenantId, false);
					boolean isActive = activePattern[tenantId][intervalId];
					int tmpload = 0;
					if (isActive) {
						tmpload = load[tenantId];
//						tmpload = (tmpload > QT) ? QT : tmpload;
					}
					out.write(" " + tmpload);
					totalLoad += tmpload;
				}
				out.write(" " + totalLoad);
				out.newLine();out.flush();
			}
		}
//		}
		out.close();
	}
	
	/**
	 * each interval is a line of workload
	 * @throws IOException
	 */
	public static void generateLoad() throws IOException{
		setBursty();
		setActivePattern(percentActive);
		FileWriter fstream = null;
		fstream = new FileWriter("load.txt", false);
		BufferedWriter out = new BufferedWriter(fstream);
		
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			out.write(""+(int)(intervalId+1));
			for(int i = 0; i < totalTenant; i++){
				if(activePattern[i][intervalId] == true){
					out.write(" "+i);
				}
			}
			out.write("\n");
		}
//		out.write("\n");
		
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			for(int min = 0; min < timePerInterval; min++){
			int[] load = new int[totalTenant];
			Random ran = new Random(System.nanoTime());
			for (int tenantId = 0; tenantId < totalTenant; tenantId++) {
				int QT = HConfig.getQT(tenantId, false);
				boolean isActive = activePattern[tenantId][intervalId];
				if (isActive) {
					if(isBursty[intervalId]){
						load[tenantId] = PossionDistribution.getRandomNumber(QT);
					}else{
						load[tenantId] = PossionDistribution.getRandomNumber(QT/4);
					}
//					load[tenantId] = ran.nextInt(QT) + 1;
//					if(isBursty[intervalId]){
//						if(QT == 200)
//							load[tenantId] = (load[tenantId]+HRan > QT)? QT :(load[tenantId]+HRan);
//						else if(QT == 60)
//							load[tenantId] = (load[tenantId] + MRan > QT) ? QT: load[tenantId] + MRan;
//						else
//							load[tenantId] = (load[tenantId] + MRan2 > QT) ? QT: load[tenantId] + MRan2;
//					}else{
//						if(QT == 200){
//							load[tenantId] = (load[tenantId] + LRan2 < 0) ? 20: load[tenantId] + LRan2;
//						}else{
//							load[tenantId] = (load[tenantId] + LRan < 0) ? 10: load[tenantId] + LRan;
//						}
//					}
				} else {
					load[tenantId] = 0;
				}
			}
//			for (int time = 0; time < timePerInterval; time++) {
//				out.write("" + (intervalId * timePerInterval + time));
//				int dd = ran.nextInt() % dRan;
			out.write(""+(intervalId*timePerInterval + min+1));
				int dd = 0;
				int totalLoad = 0;
				for (int tenantId = 0; tenantId < totalTenant; tenantId++) {
					int QT = HConfig.getQT(tenantId, false);
					boolean isActive = activePattern[tenantId][intervalId];
					int tmpload = 0;
					if (isActive) {
						tmpload = load[tenantId] + dd;
//						tmpload = (tmpload > QT) ? QT : tmpload;
					}
					out.write(" " + tmpload);
					totalLoad += tmpload;
				}
				out.write(" " + totalLoad);
				out.newLine();out.flush();
			}
		}
//		}
		out.close();
	}
	
	public static void setBursty(){
		isBursty = new boolean[totalInterval];
		for(int i = 0; i < totalInterval; i++){
			isBursty[i] = false;
		}
		isBursty[2] = isBursty[3] = isBursty[5] = true;
	}
	
	public static void setActivePattern(){
		activePattern = new boolean[totalTenant][totalInterval];
		for(int i=0;i<totalTenant;i++){
			for(int j=0;j<totalInterval;j++){
				activePattern[i][j] = false;
			}
		}
		
		activeTenant = Support.Rands(0, totalTenant, activeNumber);
		boolean[] state = new boolean[totalTenant];
		for(int i = 0; i < totalTenant; i++){
			state[i] = false;
		}
		for(int i = 0; i < activeNumber; i++){
			state[activeTenant[i]] = true;
		}
		int index = 0;
		for(int i = 0; i < totalTenant; i++){
			if(state[i] == false){
				inactiveTenant[index] = i;
				index++;
			}
		}
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			exchange();
			for(int i = 0; i < activeNumber; i++){
				activePattern[activeTenant[i]][intervalId] = true;
			}
		}
	}
	
	public static void exchange(){
		int exchangeNumber = (int) (exchangeRatio*activeNumber);
		int[] exa = Support.Rands(0, activeNumber, exchangeNumber);
		int[] exin = Support.Rands(0, inactiveTenant.length, exchangeNumber);
		int tmp;
		for(int i = 0; i < exchangeNumber; i++){
			tmp = activeTenant[exa[i]];
			activeTenant[exa[i]] = inactiveTenant[exin[i]];
			inactiveTenant[exin[i]] = tmp;
		}
	}
	/**
	 * set the firsts of each type to be active, no exhcange 
	 * @param percent
	 */
	public static void setActivePattern1(double[] percent){
		activePattern = new boolean[totalTenant][totalInterval];
		for(int i=0;i<totalTenant;i++){
			for(int j=0;j<totalInterval;j++){
				activePattern[i][j] = false;
			}
		}
//		actualTenant
		int[] actualTenantPerType = new int[18];
		int sumTmp = 0;
		for(int i = 0; i < 18; i++){
			if(i == 17){
				actualTenantPerType[17] = actualTenant - sumTmp;
			}else{
				actualTenantPerType[i] = (int) Math.round((actualTenant * HConfig.PercentTenantSplits[i]));
			}
			sumTmp += actualTenantPerType[i];
			int startId = HConfig.getStartId(i);
			for(int j = 0; j < actualTenantPerType[i]; j++){
				for(int intervalId = 0; intervalId < totalInterval; intervalId++){
					activePattern[startId+j][intervalId] = true;
				}
			}
		}
	}
	
	/**
	 * with exchange
	 * @param percent
	 */
	public static void setActivePattern(double[] percent){
		activePattern = new boolean[totalTenant][totalInterval];
		for(int i=0;i<totalTenant;i++){
			for(int j=0;j<totalInterval;j++){
				activePattern[i][j] = false;
			}
		}
		int[] tenantPerType = new int[18];
		int multi = totalTenant / 1000;
		double multid = totalTenant / 1000.0;
		for(int i = 0; i < 18; i++){
			if(totalTenant % 1000 == 0){
				tenantPerType[i] = HConfig.TenantPerType1000[i] * multi;
			}else{
				tenantPerType[i] = (int) (HConfig.TenantPerType1000[i] * multid);
			}
		}
//		int sumtmp = 0;
//		for(int i = 0; i < 17; i++){
//			tenantPerType[i] = (int) (totalTenant * HConfig.PercentTenantSplits[i]);
//			sumtmp += tenantPerType[i];
//		}
//		tenantPerType[17] = totalTenant - sumtmp;
		int[][] tenantGroup = new int[3][3];
		tenantGroup[0][0] = tenantPerType[0] + tenantPerType[1]; tenantGroup[0][1] = tenantPerType[2] + tenantPerType[3]; tenantGroup[0][2] = tenantPerType[4] + tenantPerType[5];
		tenantGroup[1][0] = tenantPerType[6] + tenantPerType[7]; tenantGroup[1][1] = tenantPerType[8] + tenantPerType[9]; tenantGroup[1][2] = tenantPerType[10] + tenantPerType[11];
		tenantGroup[2][0] = tenantPerType[12] + tenantPerType[13]; tenantGroup[2][1] = tenantPerType[14] + tenantPerType[15]; tenantGroup[2][2] = tenantPerType[16] + tenantPerType[17];
		
		int an0 = (int) (activeNumber * percent[0]);
		int an1 = (int) (activeNumber * percent[1]);
		int an2 = activeNumber - an0 - an1;
		at0 = Support.Rands(0, tenantGroup[0][0]+tenantGroup[1][0]+tenantGroup[2][0], an0);
		at1 = Support.Rands(0, tenantGroup[0][1]+tenantGroup[1][1]+tenantGroup[2][1], an1);
		at2 = Support.Rands(0, tenantGroup[0][2]+tenantGroup[1][2]+tenantGroup[2][2], an2);
		setActivePattern(at0, an0, (int)(totalTenant / 1000 * HConfig.QTPer1000[0]), tenantGroup[0][0], tenantGroup[1][0], tenantGroup[2][0], 0, HConfig.TenantIdRange[5]/3*multi, HConfig.TenantIdRange[11]/3*multi);
		setActivePattern(at1, an1, (int)(totalTenant / 1000 * HConfig.QTPer1000[1]), tenantGroup[0][1], tenantGroup[1][1], tenantGroup[2][1], HConfig.TenantIdRange[1]/3*multi, HConfig.TenantIdRange[7]/3*multi, HConfig.TenantIdRange[13]/3*multi);
		setActivePattern(at2, an2, (int)(totalTenant / 1000 * HConfig.QTPer1000[2]), tenantGroup[0][2], tenantGroup[1][2], tenantGroup[2][2], HConfig.TenantIdRange[3]/3*multi, HConfig.TenantIdRange[9]/3*multi, HConfig.TenantIdRange[15]/3*multi);
	}
	
	public static void setActivePattern(int[] a, int an, int total, int g0, int g1, int g2, int start0, int start1, int start2){
		int[] ina = new int[total - an];
		boolean[] flag = new boolean[total];
		for(int i = 0; i < total; i++)	flag[i] = false;
		for(int i = 0; i < an; i++) flag[a[i]] = true;
		int index = 0;
		for(int i = 0; i < total; i ++){
			if(flag[i] == false){
				ina[index] = i;
				index++;
			}
		}
		for(int intervalId = 0; intervalId < totalInterval; intervalId++){
			int ex = (int) (an * exchangeRatio);
			int[] tmp0 = Support.Rands(0, an, ex);
			int[] tmp1 = Support.Rands(0, total - an, ex);
			int tmp;
			for(int i = 0; i < ex; i++){
				tmp = a[tmp0[i]];
				a[tmp0[i]] = ina[tmp1[i]];
				ina[tmp1[i]] = tmp;
			}
			for(int i = 0; i < an; i++){
				if(a[i] < g0){
					activePattern[start0 + a[i]][intervalId] = true;
				}else if(a[i] < g0 + g1){
					activePattern[start1+a[i] - g0][intervalId] = true;
				}else{
					activePattern[start2+a[i] - g0 - g1 ][intervalId] = true;
				}
			}
		}
	}

}
