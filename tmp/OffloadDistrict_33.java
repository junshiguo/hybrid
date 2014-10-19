import org.voltdb.*;
import org.voltdb.types.TimestampType;
public class OffloadDistrict_33 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("insert into district33 values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
	public VoltTable[] run(int tenantId, String[] lines, int length) throws VoltAbortException {
		String[] values;		for(int i = 0; i < length; i++){
			values = lines[i].split(",");			voltQueueSQL(sql, Integer.parseInt(values[0]), Integer.parseInt(values[1]), values[2], values[3], values[4],
values[5], values[6], values[7], Double.parseDouble(values[8]), Double.parseDouble(values[9]),Integer.parseInt(values[10]), tenantId, 0, 0);
		}
		voltExecuteSQL();
		return null;
	}
}