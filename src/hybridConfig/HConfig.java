package hybridConfig;

public class HConfig {
	public static int[] ValueQT = { 20, 60, 100 };  // per minute
	public static double[] ValueDS = {6.9, 38.7, 142.4 };  // MB
	public static double[] ValueWP = { 0.6, 0.2 };  // +-0.05
	public static double[] PercentQT = { 0.3, 0.5, 0.2 };
	public static double[] PercentDS = { 0.6, 0.3, 0.1 };
	public static double[] PercentWH = { 0.4, 0.6 };  // 0.4 for write heavy and 0.6 for read heavy
	public static double[]  PercentTenantSplits  = {
		0.06	//QT: 20, DS: 6.9, write heavy
		,0.09	//QT: 20, DS: 6.9, read heavy
		,0.1		//QT: 20, DS: 38.7, write heavy
		,0.15	//QT: 20, DS: 38.7, read heavy
		,0.04	//QT: 20, DS: 142.4, write heavy
		,0.06	//QT: 20, DS: 142.4, read heavy
		
		,0.036	//QT: 60, DS: 6.9, write heavy
		,0.054	//QT: 60, DS: 6.9, read heavy
		,0.06	//QT: 60, DS: 38.7, write heavy
		,0.09	//QT: 60, DS: 38.7, read heavy
		,0.024	//QT: 60, DS: 142.4, write heavy
		,0.036	//QT: 60, DS: 142.4, write heavy
		
		,0.024	//QT: 100, DS: 6.9, write heavy
		,0.036	//QT: 100, DS: 6.9, read heavy
		,0.04	//QT: 100, DS: 38.7, write heavy
		,0.06	//QT: 100, DS: 38.7, read heavy
		,0.016	//QT: 100, DS: 142.4, write heavy
		,0.024	//QT: 100, DS: 142.4, write heavy
	};
	public static double[] PercentTenantSplitsSum = {
		0.06, 0.15, 0.25, 0.4, 0.44, 0.5,
		0.536, 0.59, 0.65, 0.74, 0.764, 0.8,
		0.824, 0.86, 0.9, 0.96, 0.976, 1.0
		};
	public static int QTMatrix[] = {ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],
		ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],
		ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],};
	public static double DSMatrix[] = {ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2],
		ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2],
		ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2]};
	public static boolean WHMatrix[] = {true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false};
	
	/**
	 * 	TYPE info according PercentTenantSplits
	 * @param tenantId
	 * @return
	 */
	public static int getType(int tenantId, boolean isType){
		return -1;
	}
	
	public static double getDS(int tenantId, boolean isType){
		return 0;
	}
	
	public static boolean isWriteHeavy(int tenantId, boolean isType){
		return true;
	}
	
	public static int getStartId(int type){
		return -1;
	}
	
}
