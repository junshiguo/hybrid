package hybridConfig;

public class HConfig {
	public static int[] ValueQT = { 60, 120, 200 };  // per minute
	public static double[] ValueDS = {3, 6.9, 18.3 };  // MB
	public static double[] ValueWP = { 0.6, 0.2 };  //
	public static double[] PercentQT = { 0.3, 0.5, 0.2 };
	public static double[] PercentDS = { 0.5, 0.3, 0.2 };
	public static double[] PercentWH = { 0.4, 0.6 };  // 0.4 for write heavy and 0.6 for read heavy
	public static double[]  PercentTenantSplits  = {
		0.06	//QT: 20, DS: 3, write heavy
		,0.09	//QT: 20, DS: 3, read heavy
		,0.036		//QT: 20, DS: 6.9, write heavy
		,0.054	//QT: 20, DS: 6.9, read heavy
		,0.024	//QT: 20, DS: 18.3, write heavy
		,0.036	//QT: 20, DS: 18.3, read heavy
		
		,0.1	//QT: 60, DS: 3, write heavy
		,0.15	//QT: 60, DS: 3, read heavy
		,0.06	//QT: 60, DS: 6.9, write heavy
		,0.09	//QT: 60, DS: 6.9, read heavy
		,0.04	//QT: 60, DS: 18.3, write heavy
		,0.06	//QT: 60, DS: 18.3, write heavy
		
		,0.04	//QT: 100, DS: 3, write heavy
		,0.06	//QT: 100, DS: 3, read heavy
		,0.024	//QT: 100, DS: 6.9, write heavy
		,0.036	//QT: 100, DS: 6.9, read heavy
		,0.016	//QT: 100, DS: 18.3, write heavy
		,0.024	//QT: 100, DS: 18.3, write heavy
	};
	public static double[] PercentTenantSplitsSum;
	public static int[] TenantIdRange;
	public static int QTMatrix[] = {ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],ValueQT[0],
		ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],ValueQT[1],
		ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],ValueQT[2],};
	public static double DSMatrix[] = {ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2],
		ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2],
		ValueDS[0],ValueDS[0],ValueDS[1],ValueDS[1],ValueDS[2],ValueDS[2]};
	public static boolean WHMatrix[] = {true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false,true,false};
	
	public static int totalTenant = 1000;
	
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
	
	public static double getDS(int tenantId, boolean isType){
		int type = tenantId;
		if(isType == false){
			type = getType(tenantId);
		}
		return DSMatrix[type];
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
