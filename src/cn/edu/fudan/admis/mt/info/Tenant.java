package cn.edu.fudan.admis.mt.info;

import java.util.Map;

public class Tenant
{
	public int serviceLevelObjective;
	public double writePercent;
	public double dataSize;
	public Map<Integer, Integer> timeWorkloadMap;

	@Override
	public String toString()
	{
		return "Tenant [serviceLevelObjective=" + serviceLevelObjective
				+ ", writePercent=" + writePercent + ", dataSize=" + dataSize
				+ ", timeWorkloadMap=" + timeWorkloadMap + "]" + "\n";
	}
}
