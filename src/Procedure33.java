

import java.math.BigDecimal;
import java.sql.SQLException;

import org.voltdb.*;

public class Procedure33 extends VoltProcedure {
	public final SQLStmt sql = new SQLStmt("UPDATE customer SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 "
			+ "WHERE tenant_id = ? AND c_id = ? AND c_d_id = ? AND c_w_id = ?");
	
	public long run(int tenant_id, double c_balance, int c_id, int c_d_id, int c_w_id) throws VoltAbortException, ClassNotFoundException, SQLException {
		voltQueueSQL(sql, new BigDecimal(c_balance).setScale(12, BigDecimal.ROUND_HALF_DOWN), tenant_id, c_id, c_d_id, c_w_id);
		VoltTable[] result = voltExecuteSQL();
		if(result[0].asScalarLong() == 0)
			return 0;
		return 1;
	}

}
