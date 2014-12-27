package edu.sjtu.se.dclab;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

public class WorkerResourceCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerResourceCalculator.class);
	
	//the scale of the weight
	private static final int SCALE_BITS = 10;
	
	private GangliaClient gangliaClient = new GangliaClient();
	private Cluster cluster;
	private GangliaCluster gangliaCluster = null;
	
	private Map<WorkerSlot, Map<ResourceType, Integer>> workerResourseWeight;
	
	public WorkerResourceCalculator(Cluster cluster){
		workerResourseWeight = new HashMap<WorkerSlot, Map<ResourceType, Integer>>();
		this.cluster = cluster;
	}
	
	public Map<WorkerSlot, Map<ResourceType, Integer>> calculateResourceWeight(){
		gangliaCluster = gangliaClient.getGangliaMetric();
		
		for(Map.Entry<String, SupervisorDetails> supervisor : cluster.getSupervisors().entrySet()){
			List<WorkerSlot> slots = cluster.getAvailableSlots(supervisor.getValue());
			
			int diskWeight = evaluateResource(ResourceType.DISK, supervisor.getValue().getHost());
			int memoryWeight = evaluateResource(ResourceType.MEMORY,supervisor.getValue().getHost());
			int cpuWeight = evaluateResource(ResourceType.CPU,supervisor.getValue().getHost());
			int networkWeight = evaluateResource(ResourceType.NETWORK,supervisor.getValue().getHost());
			
			int numberOfWorkers  = slots.size();
			
			
			for(WorkerSlot worker: slots){
				Map<ResourceType, Integer> resourseWeight  = new HashMap<ResourceType, Integer>();
				
//				resourseWeight.put(ResourceType.DISK, (diskWeight<<SCALE_BITS)/numberOfWorkers);
//				resourseWeight.put(ResourceType.CPU,  (cpuWeight<<SCALE_BITS)/numberOfWorkers);
//				resourseWeight.put(ResourceType.MEMORY, (memoryWeight<<SCALE_BITS)/numberOfWorkers);
//				resourseWeight.put(ResourceType.NETWORK, (networkWeight<<SCALE_BITS)/numberOfWorkers);
				
				resourseWeight.put(ResourceType.DISK, diskWeight/numberOfWorkers);
				resourseWeight.put(ResourceType.CPU,  cpuWeight/numberOfWorkers);
				resourseWeight.put(ResourceType.MEMORY, memoryWeight/numberOfWorkers);
				resourseWeight.put(ResourceType.NETWORK, networkWeight/numberOfWorkers);
				
				workerResourseWeight.put(worker, resourseWeight);
			}
		}
		return workerResourseWeight;
	}
	
	
	
	private int evaluateResource(ResourceType resourceType, String hostName){
		switch(resourceType){
		case DISK: return evaluateDiskWeight(hostName);
		case MEMORY: return evaluateMemoryWeight(hostName);
		case CPU: return evaluateCpuWeight(hostName);
		case NETWORK: return evaluateNetworkWeight(hostName);
		default:break;
		}
		return 0;
	}
	
	private int evaluateDiskWeight(String hostName){
		HostMetric hostMetric = gangliaCluster.getNameToHost().get(hostName);
		if (hostMetric == null) return 0;
		//get the available GB as the count
		double d = Double.parseDouble( hostMetric.getMetricMap().get(MetricConst.DISK_FREE).getVal());
		return (int)d;
	}
	
	private int evaluateNetworkWeight(String hostName){
		HostMetric hostMetric = gangliaCluster.getNameToHost().get(hostName);
		if (hostMetric == null) return 0;
		float bytesIn = Float.parseFloat( hostMetric.getMetricMap().get(MetricConst.BYTES_IN).getVal());
		float bytesOut = Float.parseFloat( hostMetric.getMetricMap().get(MetricConst.BYTES_OUT).getVal());
		//
		return 0;
	}
	
	private int evaluateCpuWeight(String hostName){
		HostMetric hostMetric = gangliaCluster.getNameToHost().get(hostName);
		if (hostMetric == null) return 0;
		//
		float cpuIdle  = Float.parseFloat( hostMetric.getMetricMap().get(MetricConst.CPU_IDLE).getVal());
		int cpuSpeed = Integer.parseInt(hostMetric.getMetricMap().get(MetricConst.CPU_SPEED).getVal());
		int cpuNum = Integer.parseInt(hostMetric.getMetricMap().get(MetricConst.CPU_NUM).getVal());
		
		return (int)(cpuSpeed * cpuNum * cpuIdle);
	}
	
	private int evaluateMemoryWeight(String hostName){
		HostMetric hostMetric = gangliaCluster.getNameToHost().get(hostName);
		if (hostMetric == null) return 0;
		//use free memory as the weight 
		float f = Float.parseFloat( hostMetric.getMetricMap().get(MetricConst.MEM_FREE).getVal());
		return (int)f;
	}
	
	
}
