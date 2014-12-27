package edu.sjtu.se.dclab;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class ResourceDemandScheduler implements IScheduler {

	private static final int DISK_WEIGHT = 4;
	private static final int MEMORY_WEIGHT = 4;
	private static final int CPU_WEIGHT = 4;
	private static final int NETWORK_WEIGHT = 4;
	private static final int DEFAULT_WEIGHT = 1;

	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceDemandScheduler.class);

	@Override
	public void prepare(Map conf) {

	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {

		// return the every with each resource weight
		Map<WorkerSlot, Map<ResourceType, Integer>> workerResourceWeight = null;
		workerResourceWeight = new WorkerResourceCalculator(cluster)
				.calculateResourceWeight();

		TopologyResourceCalculator calculator = new TopologyResourceCalculator();
		// topology id to resource need
		final Map<String, Map<ResourceType, Integer>> topologyResourceWeight = new TopologyResourceCalculator()
				.calculateResourceWeight(topologies);
		final int totalWeight = calculator.getTotalWeight();
		final Map<ResourceType, Integer> totalWeightMap = calculator.totalWeightMap;

		PriorityQueue<Map.Entry<WorkerSlot, Map<ResourceType, Integer>>> newWorkerResourceWeightQueue = new PriorityQueue<Map.Entry<WorkerSlot, Map<ResourceType, Integer>>>(
				workerResourceWeight.size(), new ResourceComparator(
						totalWeight, totalWeightMap));

		newWorkerResourceWeightQueue.addAll(workerResourceWeight.entrySet());

		//
		for (Map.Entry<String, Map<ResourceType, Integer>> topologyResourceEntry : topologyResourceWeight
				.entrySet()) {

			TopologyDetails topology = topologies.getById(topologyResourceEntry
					.getKey());
			int numOfWorkers = topology.getNumWorkers();

			boolean success = true;
			List<Map.Entry<WorkerSlot, Map<ResourceType, Integer>>> tmpList  = new ArrayList<Map.Entry<WorkerSlot, Map<ResourceType, Integer>>>();
			for (int i = 0; i < numOfWorkers; ++i) {
				Map.Entry<WorkerSlot, Map<ResourceType, Integer>> workerResource = newWorkerResourceWeightQueue.poll();
				if (workerResource == null){
					success = false;
				}else{
					tmpList.add(workerResource);
				}

				Map<ResourceType, Integer> topologyResource = topologyResourceEntry.getValue();
				//判断这个worker是否有足够的资源可以使用
				boolean available = isWorkerHasEnoughResource(workerResource.getValue(), topologyResource);
				if (!available){
					success = false;
					break;
				}
				
			}
			//如果不成功的话，把取出来的worker放回去
			if (!success){
				for(Map.Entry<WorkerSlot, Map<ResourceType, Integer>> workerResource: tmpList){
					newWorkerResourceWeightQueue.add(workerResource);
				}
				//计算下一个topology
				continue;
			}
			
			//初始化一个map，用来存储worker和executor之间的分配关系
			List<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
			Map<WorkerSlot, List<ExecutorDetails>> assignedSlots = new HashMap<WorkerSlot, List<ExecutorDetails>>(topology.getNumWorkers());
			for(Map.Entry<WorkerSlot, Map<ResourceType, Integer>> entry: tmpList){
				assignedSlots.put(entry.getKey(), new ArrayList<ExecutorDetails>());
				availableSlots.add(entry.getKey());
			}
			//将executor均匀分配到取得的多个worker上
			int slotIndex = 0;
			int size = assignedSlots.size();
			for(ExecutorDetails executor : topology.getExecutors()){
				assignedSlots.get(
						availableSlots.get(slotIndex % size)).add(executor);
				++slotIndex;
			}
			//调用cluster接口进行分配
			for(Map.Entry<WorkerSlot, List<ExecutorDetails>> e: assignedSlots.entrySet()){
				cluster.assign(e.getKey(), topology.getId(), e.getValue());
			}

		}
		
//		new EvenScheduler().schedule(topologies, cluster);
	}

	public Map<ResourceType, Integer> calculateNewWorkerResource(
			Map<ResourceType, Integer> workerResource,
			Map<ResourceType, Integer> topologyResource) {
		for (Map.Entry<ResourceType, Integer> e : workerResource.entrySet()) {
			int resourceNeeded = topologyResource.get(e.getKey());
			int newResourceVal = e.getValue() - resourceNeeded;
			workerResource.put(e.getKey(), newResourceVal);
		}
		return workerResource;
	}

	public boolean isWorkerHasEnoughResource(
			Map<ResourceType, Integer> workerResource,
			Map<ResourceType, Integer> topologyResource) {
		for (Map.Entry<ResourceType, Integer> e : workerResource.entrySet()) {
			int resourceNeeded = topologyResource.get(e.getKey());
			if (resourceNeeded > e.getValue())
				return false;
		}
		return true;
	}

	public void getWorkersBasedOnWeight() {

	}

	private class ResourceComparator implements
			Comparator<Map.Entry<WorkerSlot, Map<ResourceType, Integer>>> {

		private final int totalWeight;
		private final Map<ResourceType, Integer> totalWeightMap;

		ResourceComparator(final int totalWeight,
				final Map<ResourceType, Integer> totalWeightMap) {
			this.totalWeight = totalWeight;
			this.totalWeightMap = totalWeightMap;
		}

		@Override
		public int compare(Map.Entry<WorkerSlot, Map<ResourceType, Integer>> o1,
				Map.Entry<WorkerSlot, Map<ResourceType, Integer>> o2) {

			int scoreA = 0;
			for (Map.Entry<ResourceType, Integer> entry : o1.getValue()
					.entrySet()) {
				scoreA += (entry.getValue()
						* totalWeightMap.get(entry.getKey()) / totalWeight);
				// LOG.info("The score of Resource " + entry.getKey() + "is " +
				// );
			}

			int scoreB = 0;
			for (Map.Entry<ResourceType, Integer> entry : o2.getValue()
					.entrySet()) {
				scoreB += (entry.getValue()
						* totalWeightMap.get(entry.getKey()) / totalWeight);
				// LOG.info("The score of Resource " + entry.getKey() + "is " +
				// );
			}

			if (scoreA > scoreB)
				return -1;
			else if (scoreA < scoreB)
				return 1;
			return 0;
		}

	}

}
