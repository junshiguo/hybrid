package pooling;

import java.util.ArrayList;
import java.util.List;

public class PRequest {
	public static List<PRequest> requests = new ArrayList<PRequest>();
	
	public static synchronized PRequest gsRequest(boolean isGet, PRequest r){
		if(isGet){
			if(requests.isEmpty() == false){
				PRequest tmp = requests.get(0);
				requests.remove(0);
				return tmp;
			}
		}else{
			requests.add(r);
		}
		return null;
	}
	
	public int tenantId;
	public int sqlId;
	public Object[] para;
	public int[] paraType;
	public int paraNumber;
	public int PKNumber;
	
	public long timeStart;
	
	public PRequest(int tenantId, int sqlId, Object[] para, int[] paraType, int paraNumber, int PKNumber, long start){
		this.tenantId = tenantId;
		this.sqlId = sqlId;
		this.para = para;
		this.paraType = paraType;
		this.paraNumber = paraNumber;
		this.PKNumber = PKNumber;
		this.timeStart = start;
		PRequest.gsRequest(false, this);
	}
//	
//	public PRequest(int tenantId){
//		this.tenantId = tenantId;
//	}
//	
//	public void setMySQL(){
//		this.useVoltdb = false;
//	}
//	
//	public void setVoltDB(){
//		this.useVoltdb = true;
//	}
//	
//	public void setSQL(int sqlId, Object[] para, int[] paraType, int paraNumber){
//		this.sqlId = sqlId;
//		this.para = para;
//		this.paraType = paraType;
//		this.paraNumber = paraNumber;
//	}
//	
//	public void setSQL(int sqlId, Object[] para, int paraNumber){
//		this.sqlId = sqlId;
//		this.para = para;
//		this.paraNumber = paraNumber;
//	}
	
}
