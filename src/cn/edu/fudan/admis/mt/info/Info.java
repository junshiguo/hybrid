package cn.edu.fudan.admis.mt.info;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import hybridConfig.HConfig;

public class Info
{
	private static int allTenantNum;
	private static int activeTenantNum;
	private static String loadPath;

	public static int VoltDBMemory;
	public static double transferCostPerDataSize;
	public static int timeInterval;
	public static int maxTime;
	public static double MySQLBurstBound;

	public static List<Tenant> allTenantList;
	public static List<BurstPeriod> burstPeriodList;
	public static Map<Integer, Double> timeMySQLWritePercentAvgMap;
	public static Map<Integer, Integer> timeMySQLWorkloadSumMap;
	public static Map<Integer, List<Integer>> timeActiveTenantIdListMap;

	static final Logger logger = LogManager.getLogger(Info.class);

	// TODO
	public static void init()
	{
		allTenantNum = 3000;
//		activeTenantNum = 450;
		loadPath = "load.txt";

		VoltDBMemory = 2048;
		transferCostPerDataSize = 1.;
		timeInterval = 5;
		maxTime = 25;
		MySQLBurstBound = 1.0; 

		HConfig.init(allTenantNum);

		generateList();

		logger.debug(allTenantList.size() + " all tenant - list: "
				+ allTenantList);
		logger.debug(burstPeriodList.size() + " burst period - list: "
				+ burstPeriodList);
		logger.debug(timeMySQLWritePercentAvgMap.size()
				+ " time - MySQL write percent average - map: "
				+ timeMySQLWritePercentAvgMap);
		logger.debug(timeMySQLWorkloadSumMap.size()
				+ " time - MySQL workload sum - map: "
				+ timeMySQLWorkloadSumMap);
		logger.debug(timeActiveTenantIdListMap.size()
				+ " time - active tenant id list map: "
				+ timeActiveTenantIdListMap);
	}

	// TODO
	public static double MySQLWorkload(double writePercent)
	{
		return 29000.;
	}

	// TODO
	public static double VoltDBWorkload(double writePercent)
	{
		return 400000.;
	}

	private static void generateList()
	{
		try
		{
			allTenantList = new ArrayList<>();
			for (int i = 0; i < allTenantNum; i++)
			{
				Tenant tenant = new Tenant();
				tenant.serviceLevelObjective = HConfig.getQT(i, false);
				if (HConfig.isWriteHeavy(i, false))
					tenant.writePercent = HConfig.ValueWP[0];
				else
					tenant.writePercent = HConfig.ValueWP[1];
				tenant.dataSize = HConfig.getDS(i, false);
				tenant.timeWorkloadMap = new HashMap<>();

				allTenantList.add(tenant);
			}

			burstPeriodList = new ArrayList<>();
			timeMySQLWritePercentAvgMap = new HashMap<>();
			timeMySQLWorkloadSumMap = new HashMap<>();
			timeActiveTenantIdListMap = new HashMap<>();
			boolean burstFlag = false;
			BurstPeriod burstPeriod = null;
			BufferedReader br = new BufferedReader(new FileReader(loadPath));
			String line;
			while ((line = br.readLine()) != null)
			{
				List<Integer> activeTenantIdList = new ArrayList<>();
				String[] part = line.split(" ");
				int time = Integer.parseInt(part[0]);
				double activeWritePercentSum = 0.;
				for (int i = 1; i < part.length - 1; i++)
				{
					int workload = Integer.parseInt(part[i]);
					int tenantId = i - 1;
					allTenantList.get(tenantId).timeWorkloadMap.put(time,
							workload);
					if (workload > 0)
					{
						activeTenantIdList.add(tenantId);
						activeWritePercentSum += allTenantList.get(tenantId).writePercent;
					}
				}
				timeActiveTenantIdListMap.put(time, activeTenantIdList);

				int totalWorkload = Integer.parseInt(part[part.length - 1]);
				timeMySQLWorkloadSumMap.put(time, totalWorkload);

				double activeWritePercentAvg = activeWritePercentSum
						/ activeTenantNum;
				timeMySQLWritePercentAvgMap.put(time, activeWritePercentAvg);

				if (!burstFlag
						&& totalWorkload > (MySQLBurstBound * MySQLWorkload(activeWritePercentAvg)))
				{
					burstFlag = true;
					burstPeriod = new BurstPeriod();
					burstPeriod.burstStartTime = time;
				}
				else if (burstFlag
						&& totalWorkload <= (MySQLBurstBound * MySQLWorkload(activeWritePercentAvg)))
				{
					burstFlag = false;
					burstPeriod.burstEndTime = time;
					burstPeriodList.add(burstPeriod);
					burstPeriod = null;
				}
			}
			if (burstPeriod != null)
			{
				burstPeriod.burstEndTime = Integer.MAX_VALUE;
				burstPeriodList.add(burstPeriod);
			}
			br.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
