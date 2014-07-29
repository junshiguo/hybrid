package cn.edu.fudan.admis.mt.schedule;

import java.util.List;
import java.util.Map;

public class Known
{
	public static int VoltDBMemory;
	public static List<Tenant> allTenantList;
	public static int burstStartTime;
	public static double transferCostPerDataSize;
	public static double PerDataSize;

	public static double MySQLWorkload(double writePercent)
	{
		return 0.;
	}

	public static double VoltDBWorkload(double writePercent)
	{
		return 0.;
	}
}

class Tenant
{
	public int serviceLevelObjective;
	public double writePercent;
	public double dataSize;
	public Map<Integer, Integer> timeWorkloadMap;
}