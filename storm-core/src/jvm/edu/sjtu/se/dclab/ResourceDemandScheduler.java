package edu.sjtu.se.dclab;

import java.util.ArrayList;
import java.util.Arrays;
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
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class ResourceDemandScheduler implements IScheduler {

	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceDemandScheduler.class);

	@Override
	public void prepare(Map conf) {

	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {

		final SupervisorResourceCalculator supervisorCalculator = new SupervisorResourceCalculator(
				cluster);

		List<SupervisorResource> supervisorResources = supervisorCalculator
				.calculateSupervisorResource();

		final TopologyResourceCalculator topologyCalculator = new TopologyResourceCalculator();

		List<TopologyResource> topologyResources = topologyCalculator
				.getTopologiesResource(topologies);

		Map<String, List<WorkerSlot>> availableSlots = new HashMap<String, List<WorkerSlot>>();
		Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
		// get all the available slots of each supervisor
		for (Map.Entry<String, SupervisorDetails> entry : supervisors
				.entrySet()) {
			availableSlots.put(entry.getKey(),
					cluster.getAssignableSlots(entry.getValue()));
		}

		Map<String, List<WorkerSlot>> assignedSlots = new HashMap<String, List<WorkerSlot>>();

		for (TopologyResource tr : topologyResources) {
			// put the supervisor resource in the array for sort
			SupervisorResource[] sortedSR = new SupervisorResource[supervisorResources
					.size()];
			sortedSR = supervisorResources.toArray(sortedSR);

			List<WorkerSlot> cpuSlots = allocateCpuSlots(tr, sortedSR,availableSlots);
			List<WorkerSlot> memSlots = allocateMemSlots(tr, sortedSR,availableSlots);
			List<WorkerSlot> diskSlots = allocateDiskSlots(tr, sortedSR,availableSlots);
			List<WorkerSlot> networkSlots = allocateNetSlots(tr, sortedSR,availableSlots);
			List<WorkerSlot> commonSlots = allocateCommponSlots(tr, sortedSR,availableSlots);

			TopologyDetails td = topologies.getById(tr.getTopologyId());
			Map<ResourceType, List<ExecutorDetails>> resourceTypeToExecutors = topologyCalculator
					.calcluateExecutors(topologies.getById(tr.getTopologyId()));

			Map<WorkerSlot, List<ExecutorDetails>> result = new HashMap<WorkerSlot, List<ExecutorDetails>>();
			if (cpuSlots != null) {
				allocateExecutors(cpuSlots, resourceTypeToExecutors.get(ResourceType.CPU), result);
			}
			if (memSlots != null) {
				allocateExecutors(memSlots,resourceTypeToExecutors.get(ResourceType.MEMORY),
						result);
			}
			if (diskSlots != null) {
				allocateExecutors(diskSlots,resourceTypeToExecutors.get(ResourceType.DISK), result);
			}
			if (networkSlots != null)
				allocateExecutors(networkSlots, resourceTypeToExecutors.get(ResourceType.NETWORK),
						result);
			if (commonSlots != null){
				allocateExecutors(commonSlots, resourceTypeToExecutors.get(ResourceType.DEFAULT),
						result);
			}
			
			for (Map.Entry<WorkerSlot, List<ExecutorDetails>> e : result.entrySet()) {
				cluster.assign(e.getKey(), tr.getTopologyId(), e.getValue());
			}
		}

		// 调用cluster接口进行分配
		
		// new EvenScheduler().schedule(topologies, cluster);
	}

	public void allocateExecutors(List<WorkerSlot> workerSlots,
			List<ExecutorDetails> executors,
			Map<WorkerSlot, List<ExecutorDetails>> result) {
		int index = 0;
		while (index < executors.size()) {
			for (WorkerSlot ws : workerSlots) {
				if (index >= executors.size())
					return;
				if (result.get(ws) == null) {
					result.put(ws, new ArrayList<ExecutorDetails>());
				}
				result.get(ws).add(executors.get(index++));
			}
		}
	}

	public List<WorkerSlot> allocateCpuSlots(TopologyResource tr,
			SupervisorResource[] sortedSR,
			Map<String, List<WorkerSlot>> availableSlots) {
		if (tr.getCpuWorkers() != 0) {
			int needCpuWorkers = tr.getCpuWorkers();
			// sort by cpu load ascending
			Arrays.sort(sortedSR, new ResourceComparator.CPULoadComparator());
			LOG.info("all supervisors");
			for (SupervisorResource sr : sortedSR) {
				LOG.info("supervisor " + sr.getHostName() + " cpu load="
						+ sr.getCpuLoad());
			}
			return allocateSlots(needCpuWorkers, sortedSR, availableSlots);
		} else {
			return null;
		}
	}

	public List<WorkerSlot> allocateCommponSlots(TopologyResource tr,
			SupervisorResource[] sortedSR,
			Map<String, List<WorkerSlot>> availableSlots) {
		if (tr.getCommonWorkers() != 0) {
			int needCommonWorkers = tr.getCommonWorkers();
			List<WorkerSlot> commonSlots = new ArrayList<WorkerSlot>();
			Arrays.sort(sortedSR, new ResourceComparator.CommonComparator());
			boolean enough = false;
			while (true) {
				enough = false;
				for (SupervisorResource sr : sortedSR) {
					if (needCommonWorkers <= 0)
						return commonSlots;
					if (sr.getAvailableSlots() <= 0)
						continue;
					enough = true;
					List<WorkerSlot> aSlots = availableSlots
							.get(sr.getNodeId());
					// remove the last one
					commonSlots.add(aSlots.get(aSlots.size() - 1));
					aSlots.remove(aSlots.size() - 1);
					sr.setAvailableSlots(sr.getAvailableSlots() - 1);
				}
				if (!enough)
					throw new RuntimeException("Not enougth slots");
			}
		} else {
			return null;
		}
	}

	public List<WorkerSlot> allocateMemSlots(TopologyResource tr,
			SupervisorResource[] sortedSR,
			Map<String, List<WorkerSlot>> availableSlots) {
		if (tr.getMemWorkers() != 0) {
			int needMemWorkers = tr.getMemWorkers();
			// sort by free memory decending
			Arrays.sort(sortedSR, new ResourceComparator.MemoryFreeComparator());
			LOG.info("all supervisors");
			for (SupervisorResource sr : sortedSR) {
				LOG.info("supervisor " + sr.getHostName() + " free memory="
						+ sr.getMemFree());
			}
			return allocateSlots(needMemWorkers, sortedSR, availableSlots);
		} else {
			return null;
		}
	}

	public List<WorkerSlot> allocateDiskSlots(TopologyResource tr,
			SupervisorResource[] sortedSR,
			Map<String, List<WorkerSlot>> availableSlots) {
		if (tr.getDiskWorkers() != 0) {
			int needDiskWorkers = tr.getDiskWorkers();
			// sort by free disk decending
			Arrays.sort(sortedSR, new ResourceComparator.DiskFreeComparator());
			LOG.info("all supervisors");
			for (SupervisorResource sr : sortedSR) {
				LOG.info("supervisor " + sr.getHostName() + " free disk="
						+ sr.getDiskFree());
			}
			return allocateSlots(needDiskWorkers, sortedSR, availableSlots);
		} else {
			return null;
		}
	}

	public List<WorkerSlot> allocateNetSlots(TopologyResource tr,
			SupervisorResource[] sortedSR,
			Map<String, List<WorkerSlot>> availableSlots) {
		if (tr.getCpuWorkers() != 0) {
			int needNetWorkers = tr.getNetWorkers();
			// sort by network bytes ascending
			Arrays.sort(sortedSR, new ResourceComparator.BytesInOutComparator());
			LOG.info("all supervisors");
			for (SupervisorResource sr : sortedSR) {
				LOG.info("supervisor " + sr.getHostName()
						+ " bytes in out reate =" + sr.getBytesInOut());
			}
			return allocateSlots(needNetWorkers, sortedSR, availableSlots);
		} else {
			return null;
		}
	}

	private List<WorkerSlot> allocateSlots(int needWorkers,
			SupervisorResource[] sortedSR,
			Map<String, List<WorkerSlot>> availableSlots) {
		List<WorkerSlot> assignedSlots = new ArrayList<WorkerSlot>();
		for (SupervisorResource sr : sortedSR) {
			// available slots of this supervisor
			List<WorkerSlot> aSlots = availableSlots.get(sr.getNodeId());
			if (sr.getAvailableSlots() >= needWorkers) {
				// allocate the workers for the supervisor
				for (int i = 0; i < needWorkers; ++i) {
					// remove the last slot
					WorkerSlot ws = aSlots.get(aSlots.size() - 1);
					assignedSlots.add(ws);
					aSlots.remove(aSlots.size() - 1);
				}
				sr.setAvailableSlots(sr.getAvailableSlots() - needWorkers);
			} else {
				// add all the slots
				assignedSlots.addAll(aSlots);
				needWorkers -= sr.getAvailableSlots();
				sr.setAvailableSlots(0);
				aSlots.clear();
			}
		}
		return assignedSlots;
	}

}
