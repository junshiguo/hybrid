package hybridController;

public class VMMatch {
	public static int maxTenantPerVolumn;
	public static int[][] vmMatch;
	public static int[] tenantPerVolumn;
	public static boolean isInitiated = false;
	
	public static void init(){
		maxTenantPerVolumn = 4;
		vmMatch = new int[50][maxTenantPerVolumn];
		tenantPerVolumn = new int[100];
		for(int i = 0; i < 50; i++){
			tenantPerVolumn[i] = 0;
			for(int j = 0; j < maxTenantPerVolumn; j++){
				vmMatch[i][j] = -1;
			}
		}
		isInitiated = true;
	}
	
	public static int findTenant(int tenantId){
		for(int j = 0; j < maxTenantPerVolumn; j++){
			for(int i = 0; i < 50; i++){
				if(vmMatch[i][j] == tenantId)
					return i;
			}
		}
		return -1;
	}
	
	public static int findVolumn(){
		if(isInitiated == false){
			init();
		}
		for(int j = 0; j < maxTenantPerVolumn; j++){
			for(int i = 0; i < 50; i++){
				if(vmMatch[i][j] == -1)
					return i;
			}
		}
		return -1;
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
