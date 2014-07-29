package cn.edu.fudan.admis.mt.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Scheduler
{
	private double MySQLWritePercentAvg;
	private int MySQLServiceLevelObjectiveSum;
	private List<Integer> MySQLTenantList;
	private double VoltDBWritePercentAvg;
	private int VoltDBServiceLevelObjectiveSum;
	private double VoltDBDataSizeSum;
	private List<Integer> VoltDBTenantList;
	private int tenantViolationNum;
	private double transferCost;

	public Scheduler()
	{
		MySQLWritePercentAvg = 0.;
		MySQLServiceLevelObjectiveSum = 0;
		MySQLTenantList = new ArrayList<>();
		VoltDBWritePercentAvg = 0.;
		VoltDBServiceLevelObjectiveSum = 0;
		VoltDBDataSizeSum = 0.;
		VoltDBTenantList = new ArrayList<>();
		tenantViolationNum = 0;
		transferCost = 0.;
	}

	public void schedule()
	{
		Map<Integer, Double> activeTenantWorkloadPerDataSizeMap = generateMap();
		List<Integer> activeTenantSortList = sortByValueDesc(activeTenantWorkloadPerDataSizeMap);
		generateList(activeTenantSortList);
		transferCost = VoltDBDataSizeSum * Known.transferCostPerDataSize;
	}

	public List<Integer> getMySQLTenantList()
	{
		return MySQLTenantList;
	}

	public List<Integer> getVoltDBTenantList()
	{
		return VoltDBTenantList;
	}

	public int getTenantViolationNum()
	{
		return tenantViolationNum;
	}

	public double getTransferCost()
	{
		return transferCost;
	}

	private Map<Integer, Double> generateMap()
	{
		Map<Integer, Double> map = new HashMap<>();
		for (int i = 0; i < Known.allTenantList.size(); i++)
		{
			Tenant tenant = Known.allTenantList.get(i);
			int workload = tenant.timeWorkloadMap.get(Known.burstStartTime);
			if (workload > 0)
			{
				map.put(i, (double) workload / (double) tenant.dataSize);
			}
		}
		return map;
	}

	private List<Integer> sortByValueDesc(Map<Integer, Double> map)
	{
		List<Entry<Integer, Double>> list = new ArrayList<>(map.entrySet());
		Collections.sort(list, new Comparator<Entry<Integer, Double>>()
		{
			public int compare(Entry<Integer, Double> e1,
					Entry<Integer, Double> e2)
			{
				return (int) (e2.getValue() - e1.getValue());
			}
		});
		List<Integer> result = new ArrayList<>();
		for (Entry<Integer, Double> entry : list)
		{
			result.add(entry.getKey());
		}
		return result;
	}

	private void generateList(List<Integer> list)
	{
		int MySQLIndex = list.size() - 1;
		for (; MySQLIndex >= 0; MySQLIndex--)
		{
			int tenantId = list.get(MySQLIndex);
			if (MySQLCanHandle(tenantId))
			{
				MySQLTenantList.add(tenantId);
				continue;
			}
			else
				break;
		}
		int VoltDBIndex = 0;
		for (; VoltDBIndex <= MySQLIndex; VoltDBIndex++)
		{
			int tenantId = list.get(VoltDBIndex);
			if (VoltDBCanHandle(tenantId))
			{
				VoltDBTenantList.add(tenantId);
				continue;
			}
			else
				break;
		}
		tenantViolationNum = MySQLIndex + 1 - VoltDBIndex;
	}

	private boolean MySQLCanHandle(int index)
	{
		double writePercentAvg = (MySQLWritePercentAvg * MySQLTenantList.size() + Known.allTenantList
				.get(index).writePercent) / (MySQLTenantList.size() + 1);
		int serviceLevelObjectiveSum = MySQLServiceLevelObjectiveSum
				+ Known.allTenantList.get(index).serviceLevelObjective;
		if (serviceLevelObjectiveSum <= Known.MySQLWorkload(writePercentAvg))
		{
			MySQLWritePercentAvg = writePercentAvg;
			MySQLServiceLevelObjectiveSum = serviceLevelObjectiveSum;
			return true;
		}
		else
			return false;
	}

	private boolean VoltDBCanHandle(int index)
	{
		double writePercentAvg = (VoltDBWritePercentAvg
				* VoltDBTenantList.size() + Known.allTenantList.get(index).writePercent)
				/ (VoltDBTenantList.size() + 1);
		int serviceLevelObjectiveSum = VoltDBServiceLevelObjectiveSum
				+ Known.allTenantList.get(index).serviceLevelObjective;
		double dataSizeSum = VoltDBDataSizeSum
				+ Known.allTenantList.get(index).dataSize;
		if (dataSizeSum <= Known.VoltDBMemory
				&& (serviceLevelObjectiveSum + dataSizeSum
						* Known.transferCostPerDataSize) <= Known
							.VoltDBWorkload(writePercentAvg))
		{
			VoltDBWritePercentAvg = writePercentAvg;
			VoltDBServiceLevelObjectiveSum = serviceLevelObjectiveSum;
			VoltDBDataSizeSum = dataSizeSum;
			return true;
		}
		else
			return false;
	}
}
