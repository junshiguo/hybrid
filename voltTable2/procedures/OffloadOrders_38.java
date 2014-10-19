import org.voltdb.*;
import org.voltdb.types.TimestampType;
public class OffloadOrders_38 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("insert into orders38 values (?,?,?,?,?,?,?,?,?,?,?);");
	public VoltTable[] run(int tenantId, String[] lines, int length) throws VoltAbortException {
		String[] values;		for(int i = 0; i < length; i++){
			values = lines[i].split(",");			voltQueueSQL(sql, Integer.parseInt(values[0]), Integer.parseInt(values[1]), Integer.parseInt(values[2]), Integer.parseInt(values[3]), new TimestampType(values[4]),
					Integer.parseInt(values[5]), Integer.parseInt(values[6]), Integer.parseInt(values[7]), tenantId, 0, 0);
		}
		voltExecuteSQL();
		return null;
	}
}