package cn.edu.fudan.admis.mt.schedule.test;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.edu.fudan.admis.mt.info.BurstPeriod;
import cn.edu.fudan.admis.mt.info.Info;
import cn.edu.fudan.admis.mt.schedule.Scheduler;
import cn.edu.fudan.admis.mt.schedule.impl.DataSizeSchedulerImpl;
import cn.edu.fudan.admis.mt.schedule.impl.WorkloadPerDataSizeSchedulerImpl;
import cn.edu.fudan.admis.mt.schedule.impl.WorkloadSchedulerImpl;

public class Main
{
	static final Logger logger = LogManager.getLogger(Main.class);

	public static void main(String[] args)
	{
		Info.init();

		testWorkloadPerDataSizeSchedulerImpl();
//		testWorkloadSchedulerImpl();
//		testDataSizeSchedulerImpl();
	}

	static void testWorkloadPerDataSizeSchedulerImpl()
	{
		logger.info("test WorkloadPerDataSizeSchedulerImpl begin");
		for (BurstPeriod burstPeriod : Info.burstPeriodList)
		{
			int time = burstPeriod.burstStartTime;
			logger.info("Burst period begin: " + time);
			for (; time < burstPeriod.burstEndTime && time <= Info.maxTime; time += Info.timeInterval)
			{
				Scheduler scheduler = new WorkloadPerDataSizeSchedulerImpl(time);

				List<Integer> MySQLTenantList = scheduler.getMySQLTenantList();
				logger.info(MySQLTenantList.size()
						+ " tenants remain in MySQL: " + MySQLTenantList);

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
		logger.info("test WorkloadPerDataSizeSchedulerImpl end");
	}

	static void testWorkloadSchedulerImpl()
	{
		logger.info("test WorkloadSchedulerImpl begin");
		for (BurstPeriod burstPeriod : Info.burstPeriodList)
		{
			int time = burstPeriod.burstStartTime;
			logger.info("Burst period begin: " + time);
			for (; time < burstPeriod.burstEndTime && time <= Info.maxTime; time += Info.timeInterval)
			{
				Scheduler scheduler = new WorkloadSchedulerImpl(time);

				List<Integer> MySQLTenantList = scheduler.getMySQLTenantList();
				logger.info(MySQLTenantList.size()
						+ " tenants remain in MySQL: " + MySQLTenantList);

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
		logger.info("test WorkloadSchedulerImpl end");
	}

	static void testDataSizeSchedulerImpl()
	{
		logger.info("test DataSizeSchedulerImpl begin");
		for (BurstPeriod burstPeriod : Info.burstPeriodList)
		{
			int time = burstPeriod.burstStartTime;
			logger.info("Burst period begin: " + time);
			for (; time < burstPeriod.burstEndTime && time <= Info.maxTime; time += Info.timeInterval)
			{
				Scheduler scheduler = new DataSizeSchedulerImpl(time);

				List<Integer> MySQLTenantList = scheduler.getMySQLTenantList();
				logger.info(MySQLTenantList.size()
						+ " tenants remain in MySQL: " + MySQLTenantList);

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
		logger.info("test DataSizeSchedulerImpl end");
	}
}
