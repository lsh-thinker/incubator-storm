package edu.sjtu.se.dclab;

public class TopologyResource {
	
	private String topologyId;
	private int totalWorkers;
	private int cpuWorkers;
	private int diskWorkers;
	private int memWorkers;
	private int netWorkers;
	private int commonWorkers;
	
	public int getTotalWorkers() {
		return totalWorkers;
	}
	public void setTotalWorkers(int totalWorkers) {
		this.totalWorkers = totalWorkers;
	}
	public int getCpuWorkers() {
		return cpuWorkers;
	}
	public void setCpuWorkers(int cpuWorkers) {
		this.cpuWorkers = cpuWorkers;
	}
	public int getDiskWorkers() {
		return diskWorkers;
	}
	public void setDiskWorkers(int diskWorkers) {
		this.diskWorkers = diskWorkers;
	}
	public int getMemWorkers() {
		return memWorkers;
	}
	public void setMemWorkers(int memWorkers) {
		this.memWorkers = memWorkers;
	}
	public int getNetWorkers() {
		return netWorkers;
	}
	public void setNetWorkers(int netWorkers) {
		this.netWorkers = netWorkers;
	}
	public int getCommonWorkers() {
		return commonWorkers;
	}
	public void setCommonWorkers(int commonWorkers) {
		this.commonWorkers = commonWorkers;
	}
	public String getTopologyId() {
		return topologyId;
	}
	public void setTopologyId(String topologyId) {
		this.topologyId = topologyId;
	}
	
	
	
}
