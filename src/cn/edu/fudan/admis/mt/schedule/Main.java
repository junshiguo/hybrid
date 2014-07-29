package cn.edu.fudan.admis.mt.schedule;

import java.util.List;

public class Main
{
	public static void main(String[] args)
	{
		Scheduler scheduler = new Scheduler();
		scheduler.schedule();

		List<Integer> MySQLTenantList = scheduler.getMySQLTenantList();
		System.out.println("Tenants remain in MySQL: " + MySQLTenantList);

		List<Integer> VoltDBTenantList = scheduler.getVoltDBTenantList();
		System.out.println("Tenants transfer to VoltDB: " + VoltDBTenantList);

		int tenantViolationNum = scheduler.getTenantViolationNum();
		System.out.println("Tenants violation number: " + tenantViolationNum);

		double transferCost = scheduler.getTransferCost();
		System.out.println("Tenants transfer cost: " + transferCost);
	}
}
