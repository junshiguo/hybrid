package hybridConfig;

public class VMMatch {
	public static int maxTenantPerVolumn;
	public static int[][] vmMatch;
	public static int[] tenantPerVolumn;
	public static boolean isInitiated = false;
	
	public static void init(){
		maxTenantPerVolumn = 10;
		vmMatch = new int[100][maxTenantPerVolumn];
		tenantPerVolumn = new int[100];
		for(int i = 0; i < 100; i++){
			tenantPerVolumn[i] = 0;
			for(int j = 0; j < maxTenantPerVolumn; j++){
				vmMatch[i][j] = -1;
			}
		}
		isInitiated = true;
	}
	//add before actually offloaded data
	public static boolean addMatch(int volumnId, int tenantId){
		if(isInitiated == false){
			init();
		}
		if(tenantPerVolumn[volumnId] >= maxTenantPerVolumn){
			return false;
		}
		vmMatch[volumnId][tenantPerVolumn[volumnId]] = tenantId;
		tenantPerVolumn[volumnId] ++;
		return true;
	}
	//delete after actually retrived data
	public static boolean deleteMatch(int volumnId, int tenantId){
		if(isInitiated == false){
			init();
		}
		if(tenantPerVolumn[volumnId] <= 0){
			return false;
		}
		for(int i = 0; i < tenantPerVolumn[volumnId]; i++){
			if(vmMatch[volumnId][i] == tenantId){
				vmMatch[volumnId][i] = vmMatch[volumnId][tenantPerVolumn[volumnId] - 1];
				tenantPerVolumn[volumnId]--;
				return true;
			}
		}
		return false;
	}

}
