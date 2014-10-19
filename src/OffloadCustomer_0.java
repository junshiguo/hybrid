import org.voltdb.*;
import org.voltdb.types.TimestampType;
public class OffloadCustomer_0 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("insert into customer0 values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
	public VoltTable[] run(int tenantId, String[] lines, int length) throws VoltAbortException {
		String[] values;
		for(int i = 0; i < length; i++){
			values = lines[i].split(",");
			voltQueueSQL(sql, Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), (String)values[3], (String)values[4],
					(String)values[5], (String)values[6], (String)values[7], (String)values[8], (String)values[9],
					(String)values[10], (String)values[11], new TimestampType((String)values[12]), (String)values[13], Integer.parseInt(values[14]),
					Double.parseDouble(values[15]), Double.parseDouble(values[16]), Double.parseDouble(values[17]), Integer.parseInt(values[18]), Integer.parseInt(values[19]), 
					(String)values[20], tenantId, 0, 0);
		}
		voltExecuteSQL();
		return null;
	}
}
