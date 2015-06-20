package mysqlTest;

import newhybrid.HException;
import newhybrid.HQueryResult;
import newhybrid.HSQLTimeOutException;
import client.HTenantClient;

public class Test {
	public static void main(String[] args) throws HException, HSQLTimeOutException{
		HTenantClient htc=new HTenantClient(1);
		htc.login();
		htc.start();
		//TODO workload
		HQueryResult result;
		result=htc.sqlRandomSelect();
		result.print();
		result=htc.sqlRandomUpdate();
		result.print();
		htc.stop();
		htc.logout();
	}
}
