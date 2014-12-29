package edu.sjtu.se.dclab;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.SupervisorDetails;

public class SupervisorResourceCalculator {

	private static final Logger LOG = LoggerFactory
			.getLogger(WorkerResourceCalculator.class);

	private GangliaClient gangliaClient = new GangliaClient();
	private Cluster cluster;
	private GangliaCluster gangliaCluster = null;
	
	public SupervisorResourceCalculator(Cluster cluster){
		this.cluster = cluster;
	}
	
	public List<SupervisorResource> calculateSupervisorResource(){
		gangliaCluster = gangliaClient.getGangliaMetric();
		
		List<SupervisorResource> supervisorResources = new ArrayList<SupervisorResource>(cluster.getSupervisors().size());
		
		for(Map.Entry<String, SupervisorDetails> supervisor : cluster.getSupervisors().entrySet()){
			SupervisorResource supervisorResource = new SupervisorResource();
			HostMetric hostMetric = gangliaCluster.getNameToHost().get(supervisor.getValue().getHost());
			if (hostMetric == null)
				throw new NullPointerException("No metric for " + supervisor.getKey());
			
			supervisorResource.setHostName(supervisor.getValue().getHost());
			supervisorResource.setNodeId(supervisor.getValue().getId());
			supervisorResource.setCpuLoad(getCPULoad(hostMetric));
			supervisorResource.setMemFree(getMemFree(hostMetric));
			supervisorResource.setDiskFree(getDiskFree(hostMetric));
			supervisorResource.setBytesInOut(getBytesInOutRate(hostMetric));
			
			supervisorResources.add(supervisorResource);
		}
		
		return supervisorResources;
		
	}

	public float getCPULoad(HostMetric hostMetric) {
		float loadFifteen = Float.parseFloat(hostMetric.getMetricMap()
				.get(MetricConst.LOAD_FIFTEEN).getVal());
		int cpuNum = Integer.parseInt(hostMetric.getMetricMap()
				.get(MetricConst.CPU_NUM).getVal());
		return loadFifteen / cpuNum;
	}

	public float getMemFree(HostMetric hostMetric) {
		float f = Float.parseFloat(hostMetric.getMetricMap()
				.get(MetricConst.MEM_FREE).getVal());
		return f;
	}

	public double getDiskFree(HostMetric hostMetric) {
		double d = Double.parseDouble(hostMetric.getMetricMap()
				.get(MetricConst.DISK_FREE).getVal());
		return d;
	}

	public float getBytesInOutRate(HostMetric hostMetric) {
		float bytesIn = Float.parseFloat(hostMetric.getMetricMap()
				.get(MetricConst.BYTES_IN).getVal());
		float bytesOut = Float.parseFloat(hostMetric.getMetricMap()
				.get(MetricConst.BYTES_OUT).getVal());
		return (bytesIn + bytesOut) / 2;
	}
	
}
