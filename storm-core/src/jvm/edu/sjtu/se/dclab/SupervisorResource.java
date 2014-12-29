package edu.sjtu.se.dclab;

public class SupervisorResource {
	
	private int availableSlots;
	private String hostName;
	private String nodeId;
	private float cpuLoad;
	private double diskFree;
	private double memFree;
	private float bytesInOut;
	
	
	public float getCpuLoad() {
		return cpuLoad;
	}
	public void setCpuLoad(float cpuLoad) {
		this.cpuLoad = cpuLoad;
	}
	public double getDiskFree() {
		return diskFree;
	}
	public void setDiskFree(double diskFree) {
		this.diskFree = diskFree;
	}
	public double getMemFree() {
		return memFree;
	}
	public void setMemFree(double memFree) {
		this.memFree = memFree;
	}
	public float getBytesInOut() {
		return bytesInOut;
	}
	public void setBytesInOut(float bytesInOut) {
		this.bytesInOut = bytesInOut;
	}
	public int getAvailableSlots() {
		return availableSlots;
	}
	public void setAvailableSlots(int availableSlots) {
		this.availableSlots = availableSlots;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	
	
}
