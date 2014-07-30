package cn.edu.fudan.admis.mt.schedule;

import java.util.List;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main
{
	static final Logger logger = LogManager.getLogger(Main.class);

	public static void main(String[] args)
	{
		Known.init();

		for (BurstPeriod burstPeriod : Known.burstPeriodList)
		{
			int time = burstPeriod.burstStartTime;
			logger.info("Burst period begin: " + time);
			for (; time < burstPeriod.burstEndTime && time <= Known.maxTime; time += Known.timeInterval)
			{
				Scheduler scheduler = new Scheduler(time);
				scheduler.schedule();

				Queue<Integer> MySQLTenantQueue = scheduler
						.getMySQLTenantQueue();
				logger.info(MySQLTenantQueue.size()
						+ " tenants remain in MySQL: " + MySQLTenantQueue);

				List<Integer> VoltDBTenantList = scheduler
						.getVoltDBTenantList();
				logger.info(VoltDBTenantList.size()
						+ " tenants transfer to VoltDB: " + VoltDBTenantList);

				List<Integer> violationTenantList = scheduler
						.getViolationTenantList();
				logger.info(violationTenantList.size()
						+ " tenants violation : " + violationTenantList);

				double transferCost = scheduler.getTransferCost();
				logger.info("Tenants transfer cost: " + transferCost);
			}
			logger.info("Burst period end: " + time);
		}
	}
}
