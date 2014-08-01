package cn.edu.fudan.admis.mt.schedule;

import java.util.List;

public interface Scheduler
{
	public List<Integer> getMySQLTenantList();

	public List<Integer> getVoltDBTenantList();

	public List<Integer> getViolationTenantList();

	public double getTransferCost();
}
