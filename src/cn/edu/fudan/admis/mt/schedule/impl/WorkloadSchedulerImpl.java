package cn.edu.fudan.admis.mt.schedule.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import cn.edu.fudan.admis.mt.info.Info;
import cn.edu.fudan.admis.mt.info.Tenant;
import cn.edu.fudan.admis.mt.util.Util;

public class WorkloadSchedulerImpl extends BaseSchedulerImpl
{
	public WorkloadSchedulerImpl(int burstTime)
	{
		super(burstTime);
	}

	@Override
	protected Queue<Integer> generateQueue()
	{
		Map<Integer, Integer> map = new HashMap<>();
		int activeTenantNum = Info.timeActiveTenantIdListMap.get(burstTime)
				.size();
		for (int i = 0; i < activeTenantNum; i++)
		{
			int activeTenantId = Info.timeActiveTenantIdListMap.get(burstTime)
					.get(i);
			Tenant activeTenant = Info.allTenantList.get(activeTenantId);
			int workload = activeTenant.timeWorkloadMap.get(burstTime);
			map.put(activeTenantId, workload);
		}
		return Util.getKey(Util.sortByValue(map, false));
	}
}
