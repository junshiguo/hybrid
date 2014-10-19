import org.voltdb.*;
import org.voltdb.types.TimestampType;
public class OffloadCustomer_6 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("insert into customer6 values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
	public VoltTable[] run(int tenantId, String[] lines, int length) throws VoltAbortException {
		String[] values;		for(int i = 0; i < length; i++){
			values = lines[i].split(",");			voltQueueSQL(sql, Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), values[3], values[4],
					values[5], values[6], values[7], values[8], values[9],
					values[10], values[11], new TimestampType(values[12]), values[13], Integer.parseInt(values[14]),
					Double.parseDouble(values[15]), Double.parseDouble(values[16]), Double.parseDouble(values[17]), Integer.parseInt(values[18]), Integer.parseInt(values[19]),
					values[20], tenantId, 0, 0);
		}
		voltExecuteSQL();
		return null;
	}
}