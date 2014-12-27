package edu.sjtu.se.dclab;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Utils;

public class SingleNodeScheduler implements IScheduler{
	
    private static final Logger LOG = LoggerFactory.getLogger(SingleNodeScheduler.class);

	@Override
	public void prepare(Map conf) {
		
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		
		
		List<TopologyDetails> singleNodeTopologies = new ArrayList<TopologyDetails>();
//		Map<String, Topology> 
		
		//get all the topology that need to run on a single supervisor node
		for(TopologyDetails topology: cluster.needsSchedulingTopologies(topologies)){
			Object obj = topology.getConf().get(Config.STORM_TOPOLOGY_SINGLE_NODE);
			boolean singleNode = Utils.getBoolean(obj, false);
			if (singleNode){
				singleNodeTopologies.add(topology);
			}
		}
		
		//return a mapping from topology id to supervisor id
		Map<String, String> topologyToSupervisor = findProperSupervisor(singleNodeTopologies, cluster);
		
		
		for(Map.Entry<String, String> entry: topologyToSupervisor.entrySet()){
			SupervisorDetails supervisor = cluster.getSupervisors().get(entry.getValue());
			TopologyDetails topology = topologies.getById(entry.getKey());
			if (topology == null)
				LOG.info("Topology not exist: " + entry.getKey());
			if (supervisor == null)
				LOG.info("Supervisor not exist:" + entry.getValue());
			
			Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
			
			
			List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);
			Map<WorkerSlot, List<ExecutorDetails>> assignedSlots = new HashMap<WorkerSlot, List<ExecutorDetails>>(topology.getNumWorkers());
			for(WorkerSlot slot: availableSlots){
				assignedSlots.put(slot, new ArrayList<ExecutorDetails>());
			}
			
			int slotIndex = 0;
			int size = assignedSlots.size();
			for(Map.Entry<String, List<ExecutorDetails>> e: componentToExecutors.entrySet()){
				for(ExecutorDetails executor: e.getValue()){
					assignedSlots.get(
							availableSlots.get(slotIndex % size)).add(executor);
					++slotIndex;
				}
			}
			
			for(Map.Entry<WorkerSlot, List<ExecutorDetails>> e: assignedSlots.entrySet()){
				cluster.assign(e.getKey(), topology.getId(), e.getValue());
			}

		}
		
		
//		new EvenScheduler().schedule(topologies, cluster);
		
	}
	
	/**
	 * the Topology name and Supervisor mapping
	 * @return
	 */
	private Map<String, String> findProperSupervisor(List<TopologyDetails> singleNodeTopologies, Cluster cluster){
		Map<String, String> topologyToSupervisor = new HashMap<String, String>();
		
		//priority queue to store supervisor node
		Queue<SupervisorSlots> supervisorQueue = new PriorityQueue<SupervisorSlots>();
		for(Map.Entry<String, SupervisorDetails> entry: cluster.getSupervisors().entrySet()){
			supervisorQueue.add(new SupervisorSlots(entry.getKey(), cluster.getAvailableSlots(entry.getValue()).size() ));
		}
		
		//priority queue to store topology 
		Queue<TopologyWorkers> topologyQueue = new PriorityQueue<TopologyWorkers>();
		for(TopologyDetails topology : singleNodeTopologies){
			topologyQueue.add(new TopologyWorkers(topology.getId(), topology.getNumWorkers()));
		}
		
		//record slot number that each topology needs
		TopologyWorkers tWorker = null;
		while ((tWorker = topologyQueue.poll()) != null) {
			if (supervisorQueue.size() == 0) break;
			//get the supervisor node with most slot
			SupervisorSlots sSlots = supervisorQueue.element();
			if (tWorker.needWorkers <= sSlots.availableSlots){
				sSlots = supervisorQueue.poll();
				topologyToSupervisor.put(tWorker.topologyId, sSlots.supervisorId);
				sSlots.availableSlots = sSlots.availableSlots - tWorker.needWorkers;
				//add back to the supervisor queue
				supervisorQueue.add(sSlots);
			}
		}
		//LOG
		for(Map.Entry<String, String> entry : topologyToSupervisor.entrySet()){
			LOG.info("Topology:" + entry.getKey() + " -> " + " Supervisor:" + entry.getValue());
		}
		return topologyToSupervisor;
	}
	
	private static class TopologyWorkers implements Comparable<TopologyWorkers>{
		
		private String topologyId;
		private int needWorkers;
		
		TopologyWorkers(String topologyId, int needWorkers){
			this.topologyId = topologyId;
			this.needWorkers = needWorkers;
		}
		
		@Override
		public int compareTo(TopologyWorkers o) {
			if (needWorkers < o.needWorkers){
				return 1;
			}else if (needWorkers > o.needWorkers){
				return -1;
			}else{
				return 0;
			}
		}
	}
	
	private static class SupervisorSlots implements Comparable<SupervisorSlots>{
		
		private String supervisorId;
		private int availableSlots;
		
		SupervisorSlots(String supervisorId, int availableSlots){
			this.supervisorId = supervisorId;
			this.availableSlots = availableSlots;
		}
		
		@Override
		public int compareTo(SupervisorSlots o) {
			if (availableSlots < o.availableSlots){
				return 1;
			}else if (availableSlots > o.availableSlots){
				return -1;
			}else{
				return 0;
			}
		}
	}
	
	public static void main(String[] args){
		
	}
	
	
}


