package cn.edu.fudan.admis.mt.schedule.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import cn.edu.fudan.admis.mt.info.Info;
import cn.edu.fudan.admis.mt.schedule.Scheduler;

public abstract class BaseSchedulerImpl implements Scheduler
{
	protected int burstTime;

	private double MySQLWritePercentAvg;
	private int MySQLWorkloadSum;

	private double VoltDBWritePercentAvg;
	private int VoltDBWorkloadSum;
	private double VoltDBDataSizeSum;
	private double transferCost;

	private Queue<Integer> MySQLTenantQueue;
	private List<Integer> VoltDBTenantList;
	private List<Integer> violationTenantList;

	public BaseSchedulerImpl(int burstTime)
	{
		this.burstTime = burstTime;

		MySQLWritePercentAvg = Info.timeMySQLWritePercentAvgMap.get(burstTime);
		MySQLWorkloadSum = Info.timeMySQLWorkloadSumMap.get(burstTime);

		VoltDBWritePercentAvg = 0.;
		VoltDBWorkloadSum = 0;
		VoltDBDataSizeSum = 0.;
		transferCost = 0.;

		schedule();
	}

	@Override
	public List<Integer> getMySQLTenantList()
	{
		return new ArrayList<>(MySQLTenantQueue);
	}

	@Override
	public List<Integer> getVoltDBTenantList()
	{
		return VoltDBTenantList;
	}

	@Override
	public List<Integer> getViolationTenantList()
	{
		return violationTenantList;
	}

	@Override
	public double getTransferCost()
	{
		return transferCost;
	}

	private void schedule()
	{
		MySQLTenantQueue = generateQueue();

		transfer();

		transferCost = VoltDBDataSizeSum * Info.transferCostPerDataSize;
	}

	protected abstract Queue<Integer> generateQueue();

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
				* (MySQLTenantQueue.size() + 1) - Info.allTenantList
					.get(tenantId).writePercent) / MySQLTenantQueue.size();
		MySQLWorkloadSum = MySQLWorkloadSum
				- Info.allTenantList.get(tenantId).timeWorkloadMap
						.get(burstTime);
		if (MySQLWorkloadSum <= (Info.MySQLBurstBound * Info
				.MySQLWorkload(MySQLWritePercentAvg)))
			return true;
		else
			return false;
	}

	private boolean VoltDBCanHandleWhenAdd(int tenantId)
	{
		double writePercentAvg = (VoltDBWritePercentAvg
				* VoltDBTenantList.size() + Info.allTenantList.get(tenantId).writePercent)
				/ (VoltDBTenantList.size() + 1);
		int workloadSum = VoltDBWorkloadSum
				+ Info.allTenantList.get(tenantId).timeWorkloadMap
						.get(burstTime);
		double dataSizeSum = VoltDBDataSizeSum
				+ Info.allTenantList.get(tenantId).dataSize;
		if (dataSizeSum <= Info.VoltDBMemory
				&& (workloadSum + dataSizeSum * Info.transferCostPerDataSize) <= Info
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
