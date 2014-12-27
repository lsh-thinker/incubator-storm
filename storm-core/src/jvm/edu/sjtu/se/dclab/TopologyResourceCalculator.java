package edu.sjtu.se.dclab;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONValue;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

public class TopologyResourceCalculator {
	
	private int totalWeight = 0;
	Map<ResourceType, Integer> totalWeightMap;
	
	public TopologyResourceCalculator(){
		totalWeightMap = new HashMap<ResourceType,Integer>(4);
		totalWeightMap.put(ResourceType.DISK, 0);
		totalWeightMap.put(ResourceType.CPU, 0);
		totalWeightMap.put(ResourceType.MEMORY, 0);
		totalWeightMap.put(ResourceType.NETWORK, 0);
	}
	
	
	
	public Map<String, Map<ResourceType, Integer>> calculateResourceWeight(Topologies topologies){
		Map<String, Map<ResourceType, Integer>> topologyResourceWeight = new HashMap<String, Map<ResourceType, Integer>>();
		for(TopologyDetails topology: topologies.getTopologies()){
			topologyResourceWeight.put(topology.getId(), calculateResouceWeight(topology));
		}
		return topologyResourceWeight;  
	}
	
	private Map<ResourceType,  Integer> calculateResouceWeight(TopologyDetails topology){
		Map<ResourceType, Integer> resourceWeight = new HashMap<ResourceType, Integer>();
		
		resourceWeight.put(ResourceType.DISK, 0);
		resourceWeight.put(ResourceType.CPU, 0);
		resourceWeight.put(ResourceType.MEMORY, 0);
		resourceWeight.put(ResourceType.NETWORK, 0);
		
		
		
		Map<String, SpoutSpec> spoutMap = topology.getTopology().get_spouts();
		Map<String, Bolt> boltMap = topology.getTopology().get_bolts();
		
		for(Map.Entry<String, SpoutSpec> spout: spoutMap.entrySet()){
			ComponentCommon common = spout.getValue().get_common();
			String jsonConf = common.get_json_conf();
			ResourceType rt = ResourceType.DEFAULT;
			if(jsonConf!=null) {
                Map conf = (Map) JSONValue.parse(jsonConf);
                rt = (ResourceType)conf.get(Config.STORM_COMPONENT_RESOURCE_TYPE);
            }
			
			if (!rt.equals(ResourceType.DEFAULT)){
				Integer val = common.get_parallelism_hint()<<2;
				//the weight is equal to parallelism * 4, count every component
				resourceWeight.put(rt, resourceWeight.get(rt) + val);
				totalWeightMap.put(rt, totalWeightMap.get(rt) + val);
				
			}else{
				//DEFAULT, add to every resource
				final int parallelism = common.get_parallelism_hint();
				resourceWeight.put(ResourceType.DISK, resourceWeight.get(ResourceType.DISK) + parallelism);
				resourceWeight.put(ResourceType.CPU, resourceWeight.get(ResourceType.CPU) + parallelism);
				resourceWeight.put(ResourceType.MEMORY, resourceWeight.get(ResourceType.MEMORY) + parallelism);
				resourceWeight.put(ResourceType.NETWORK, resourceWeight.get(ResourceType.NETWORK) + parallelism);
				
				totalWeightMap.put(ResourceType.DISK, totalWeightMap.get(ResourceType.DISK) + parallelism);
				totalWeightMap.put(ResourceType.CPU, totalWeightMap.get(ResourceType.CPU) + parallelism);
				totalWeightMap.put(ResourceType.MEMORY, totalWeightMap.get(ResourceType.MEMORY) + parallelism);
				totalWeightMap.put(ResourceType.NETWORK, totalWeightMap.get(ResourceType.NETWORK) + parallelism);
			
			}
			totalWeight += (common.get_parallelism_hint()<<2);
		}
		
		for(Map.Entry<String, Bolt> bolt: boltMap.entrySet()){
			ComponentCommon common = bolt.getValue().get_common();
			String jsonConf = common.get_json_conf();
			ResourceType rt = ResourceType.DEFAULT;
			
			if(jsonConf!=null) {
                Map conf = (Map) JSONValue.parse(jsonConf);
                rt = (ResourceType)conf.get(Config.STORM_COMPONENT_RESOURCE_TYPE);
            }
			
			if (!rt.equals(ResourceType.DEFAULT)){
				Integer val = common.get_parallelism_hint()<<2;
				//the weight is equal to parallelism * 4, count every component
				resourceWeight.put(rt, resourceWeight.get(rt) + val);
				totalWeightMap.put(rt, totalWeightMap.get(rt) + val);
			}else{
				final int parallelism = common.get_parallelism_hint();
				resourceWeight.put(ResourceType.DISK, resourceWeight.get(ResourceType.DISK) + parallelism);
				resourceWeight.put(ResourceType.CPU, resourceWeight.get(ResourceType.CPU) + parallelism);
				resourceWeight.put(ResourceType.MEMORY, resourceWeight.get(ResourceType.MEMORY) + parallelism);
				resourceWeight.put(ResourceType.NETWORK, resourceWeight.get(ResourceType.NETWORK) + parallelism);
				
				totalWeightMap.put(ResourceType.DISK, totalWeightMap.get(ResourceType.DISK) + parallelism);
				totalWeightMap.put(ResourceType.CPU, totalWeightMap.get(ResourceType.CPU) + parallelism);
				totalWeightMap.put(ResourceType.MEMORY, totalWeightMap.get(ResourceType.MEMORY) + parallelism);
				totalWeightMap.put(ResourceType.NETWORK, totalWeightMap.get(ResourceType.NETWORK) + parallelism);
			}
			totalWeight += (common.get_parallelism_hint()<<2);
			
			
		}
		final int numOfWorkers = topology.getNumWorkers();
		resourceWeight.put(ResourceType.DISK, resourceWeight.get(ResourceType.DISK)/numOfWorkers);
		resourceWeight.put(ResourceType.CPU, resourceWeight.get(ResourceType.CPU)/numOfWorkers);
		resourceWeight.put(ResourceType.MEMORY, resourceWeight.get(ResourceType.MEMORY)/numOfWorkers);
		resourceWeight.put(ResourceType.NETWORK, resourceWeight.get(ResourceType.NETWORK)/numOfWorkers);
		
		return resourceWeight;
	}



	public int getTotalWeight() {
		return totalWeight;
	}
	
	
	
	
}
