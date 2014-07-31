package cn.edu.fudan.admis.mt.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Scheduler
{
	private int burstTime;

	private double MySQLWritePercentAvg;
	private int MySQLWorkloadSum;

	private double VoltDBWritePercentAvg;
	private int VoltDBWorkloadSum;
	private double VoltDBDataSizeSum;
	private double transferCost;

	private Queue<Integer> MySQLTenantQueue;
	private List<Integer> VoltDBTenantList;
	private List<Integer> violationTenantList;

	static final Logger logger = LogManager.getLogger(Scheduler.class);

	public Scheduler(int burstTime)
	{
		this.burstTime = burstTime;

		MySQLWritePercentAvg = Known.timeMySQLWritePercentAvgMap.get(burstTime);
		MySQLWorkloadSum = Known.timeMySQLWorkloadSumMap.get(burstTime);

		VoltDBWritePercentAvg = 0.;
		VoltDBWorkloadSum = 0;
		VoltDBDataSizeSum = 0.;
		transferCost = 0.;
	}

	public void schedule()
	{
		Map<Integer, Double> activeTenantWorkloadPerDataSizeMap = generateMap();
		logger.debug(activeTenantWorkloadPerDataSizeMap.size()
				+ " active tenant - workload per data size - map: "
				+ activeTenantWorkloadPerDataSizeMap);

		MySQLTenantQueue = sortByValueDesc(activeTenantWorkloadPerDataSizeMap);

		transfer();

		transferCost = VoltDBDataSizeSum * Known.transferCostPerDataSize;
	}

	public Queue<Integer> getMySQLTenantQueue()
	{
		return MySQLTenantQueue;
	}

	public List<Integer> getVoltDBTenantList()
	{
		return VoltDBTenantList;
	}

	public double getTransferCost()
	{
		return transferCost;
	}

	public List<Integer> getViolationTenantList()
	{
		return violationTenantList;
	}

	private Map<Integer, Double> generateMap()
	{
		Map<Integer, Double> map = new HashMap<>();
		int activeTenantNum = Known.timeActiveTenantIdListMap.get(burstTime)
				.size();
		for (int i = 0; i < activeTenantNum; i++)
		{
			int activeTenantId = Known.timeActiveTenantIdListMap.get(burstTime)
					.get(i);
			Tenant activeTenant = Known.allTenantList.get(activeTenantId);
			int workload = activeTenant.timeWorkloadMap.get(burstTime);
			map.put(activeTenantId, (double) workload
					/ (double) activeTenant.dataSize);
		}
		return map;
	}

	private Queue<Integer> sortByValueDesc(Map<Integer, Double> map)
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
		logger.debug(list.size()
				+ " active tenant id - list - sort by workload per data size desc: "
				+ list);

		Queue<Integer> result = new LinkedList<>();
		for (Entry<Integer, Double> entry : list)
		{
			result.offer(entry.getKey());
		}
		return result;
	}

	private void transfer()
	{
		VoltDBTenantList = new ArrayList<>();
		violationTenantList = new ArrayList<>();
		while (true)
		{
			int tenantId = MySQLTenantQueue.poll();
			if (VoltDBCanHandleWhenAdd(tenantId))
				VoltDBTenantList.add(tenantId);
			else
				violationTenantList.add(tenantId);
			if (MySQLCanHandleAfterRemove(tenantId))
				break;
		}
	}

	private boolean MySQLCanHandleAfterRemove(int tenantId)
	{
		MySQLWritePercentAvg = (MySQLWritePercentAvg
				* (MySQLTenantQueue.size() + 1) - Known.allTenantList
					.get(tenantId).writePercent) / MySQLTenantQueue.size();
		MySQLWorkloadSum = MySQLWorkloadSum
				- Known.allTenantList.get(tenantId).timeWorkloadMap
						.get(burstTime);
		if (MySQLWorkloadSum <= (Known.MySQLBurstScale * Known
				.MySQLWorkload(MySQLWritePercentAvg)))
			return true;
		else
			return false;
	}

	private boolean VoltDBCanHandleWhenAdd(int tenantId)
	{
		double writePercentAvg = (VoltDBWritePercentAvg
				* VoltDBTenantList.size() + Known.allTenantList.get(tenantId).writePercent)
				/ (VoltDBTenantList.size() + 1);
		int workloadSum = VoltDBWorkloadSum
				+ Known.allTenantList.get(tenantId).timeWorkloadMap
						.get(burstTime);
		double dataSizeSum = VoltDBDataSizeSum
				+ Known.allTenantList.get(tenantId).dataSize;
		if (dataSizeSum <= Known.VoltDBMemory
				&& (workloadSum + dataSizeSum * Known.transferCostPerDataSize) <= Known
						.VoltDBWorkload(writePercentAvg))
		{
			VoltDBWritePercentAvg = writePercentAvg;
			VoltDBWorkloadSum = workloadSum;
			VoltDBDataSizeSum = dataSizeSum;
			return true;
		}
		else
			return false;
	}
}
