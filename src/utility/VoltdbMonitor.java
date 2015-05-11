package utility;

import java.io.IOException;

import org.voltdb.*;
import org.voltdb.client.*;

public class VoltdbMonitor {
	public static void main(String[] args){
		Client conn = DBManager.connectVoltdb("10.20.2.220");
		try {
			getMem(conn);
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
		}
	}
	
	public static void getMem(Client conn) throws NoConnectionsException, IOException, ProcCallException{
		VoltTable[] result = conn.callProcedure("@Statistics", "MEMORY", 0).getResults();
		int javaUsed = 0, javaUnused = 0, tupleCount = 0;
		int tupleData = 0, tupleAllocated = 0, indexMemory = 0;
		int stringMemory = 0, pooledMemory = 0, rss = 0;
		for(VoltTable table : result){
			for(int i=0; i<table.getRowCount(); i++){
				VoltTableRow row = table.fetchRow(i);
				javaUsed += (int) row.get("JAVAUSED", VoltType.INTEGER);
				javaUnused += (int) row.get("JAVAUNUSED", VoltType.INTEGER);
				tupleCount += (int) row.get("TUPLECOUNT", VoltType.INTEGER);
				tupleData += (int) row.get("TUPLEDATA", VoltType.INTEGER);
				tupleAllocated += (int) row.get("TUPLEALLOCATED", VoltType.INTEGER);
				indexMemory += (int) row.get("INDEXMEMORY", VoltType.INTEGER);
				stringMemory += (int) row.get("STRINGMEMORY", VoltType.INTEGER);
				pooledMemory += (int) row.get("POOLEDMEMORY", VoltType.INTEGER);
				rss += (int) row.get("RSS", VoltType.INTEGER);
			}
		}
		System.out.println("java used : "+javaUsed/1024.0+" MB");
		System.out.println("java unused: "+javaUnused/1024.0+" MB");
		System.out.println("tuple count: "+tupleCount/1024.0);
		System.out.println("tuple data: "+tupleData/1024.0+" MB");
		System.out.println("tuple allocated: "+tupleAllocated/1024.0+" MB");
		System.out.println("index memory: "+indexMemory/1024.0+" MB");
		System.out.println("string memory: "+stringMemory/1024.0+" MB");
		System.out.println("pooled memory: "+pooledMemory/1024.0+" MB");
		System.out.println("rss: "+rss/1024.0+" MB");
	}
	
}
