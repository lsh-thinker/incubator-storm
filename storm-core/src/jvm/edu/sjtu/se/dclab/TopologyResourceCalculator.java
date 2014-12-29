package edu.sjtu.se.dclab;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

public class TopologyResourceCalculator {
	

	
	public Map<ResourceType, List<ExecutorDetails>> calcluateExecutors(TopologyDetails topology){
		List<ExecutorDetails> cpuExecutors = new ArrayList<ExecutorDetails>();
		List<ExecutorDetails> memExecutors = new ArrayList<ExecutorDetails>();
		List<ExecutorDetails> diskExecutors = new ArrayList<ExecutorDetails>();
		List<ExecutorDetails> netExecutors = new ArrayList<ExecutorDetails>();
		List<ExecutorDetails> commonExecutors = new ArrayList<ExecutorDetails>();
		
		Map<String, SpoutSpec> spoutMap = topology.getTopology().get_spouts();
		Map<String, Bolt> boltMap = topology.getTopology().get_bolts();
		
		
		Map<ResourceType, List<ExecutorDetails>>  result = new HashMap<ResourceType, List<ExecutorDetails>>();
		for(Map.Entry<ExecutorDetails, String> entry: topology.getExecutorToComponent().entrySet()){
			String componentId = entry.getValue();
			ResourceType rt = getResourceTypeOfComponent(componentId,spoutMap,boltMap);
			ExecutorDetails executor = entry.getKey();
			switch(rt){
			case CPU: cpuExecutors.add(executor); break;
			case MEMORY: memExecutors.add(executor);break;
			case DISK: diskExecutors.add(executor); break;
			case NETWORK: netExecutors.add(executor); break;
			case DEFAULT: commonExecutors.add(executor);break;
			}
		}
		result.put(ResourceType.CPU, cpuExecutors);
		result.put(ResourceType.MEMORY, memExecutors);
		result.put(ResourceType.DISK, diskExecutors);
		result.put(ResourceType.NETWORK, netExecutors);
		result.put(ResourceType.DEFAULT, commonExecutors);
		return result;
	}
	
	public TopologyResourceCalculator(){
		
	}
	
	private ResourceType getResourceTypeOfComponent(String componentId, Map<String, SpoutSpec> spoutMap, Map<String, Bolt> blotMap){
		SpoutSpec spout = spoutMap.get(componentId);
		if (spout != null) {
			ComponentCommon common = spout.get_common();
			String jsonConf = common.get_json_conf();
			ResourceType rt = ResourceType.DEFAULT;
			if(jsonConf!=null) {
	            Map conf = (Map) JSONValue.parse(jsonConf);
	            rt = (ResourceType)conf.get(Config.STORM_COMPONENT_RESOURCE_TYPE);
	        }
			return rt;
		} else{
			Bolt bolt = blotMap.get(componentId);
			if (bolt == null) throw new NullPointerException("ComponentId not exit " + componentId);
			ComponentCommon common = bolt.get_common();
			String jsonConf = common.get_json_conf();
			ResourceType rt = ResourceType.DEFAULT;
			if(jsonConf!=null) {
	            Map conf = (Map) JSONValue.parse(jsonConf);
	            rt = (ResourceType)conf.get(Config.STORM_COMPONENT_RESOURCE_TYPE);
	        }
			return rt;
		}

	}
	
	public List<TopologyResource> getTopologiesResource(Topologies topologies){
		List<TopologyResource> topologyResources = new ArrayList<TopologyResource>();
		for(TopologyDetails topology: topologies.getTopologies()){
			TopologyResource tr = calculateToplogyResource(topology);
			topologyResources.add(tr);
		}
		return topologyResources;  
	}
	
	private TopologyResource calculateToplogyResource(TopologyDetails topology){
		Map<String, SpoutSpec> spoutMap = topology.getTopology().get_spouts();
		Map<String, Bolt> boltMap = topology.getTopology().get_bolts();
		
		int totalExecutors = 0;
		int diskExecutors = 0;
		int cpuExecutors = 0;
		int memExecutors = 0;
		int networkExecutors = 0;
		int commonExecutors = 0;
		
		for(Map.Entry<String, SpoutSpec> spout: spoutMap.entrySet()){
			ComponentCommon common = spout.getValue().get_common();
			String jsonConf = common.get_json_conf();
			ResourceType rt = ResourceType.DEFAULT;
			if(jsonConf!=null) {
                Map conf = (Map) JSONValue.parse(jsonConf);
                rt = (ResourceType)conf.get(Config.STORM_COMPONENT_RESOURCE_TYPE);
            }
			
			int exec = common.get_parallelism_hint();
			totalExecutors += exec;
			switch(rt){
			case CPU: cpuExecutors+=exec;break;
			case MEMORY: memExecutors+=exec;break;
			case DISK: diskExecutors+=exec;break;
			case NETWORK: networkExecutors+=exec;break;
			case DEFAULT: commonExecutors+=exec;break;
			}
		}
		
		for(Map.Entry<String,Bolt> bolt: boltMap.entrySet()){
			ComponentCommon common = bolt.getValue().get_common();
			String jsonConf = common.get_json_conf();
			ResourceType rt = ResourceType.DEFAULT;
			if(jsonConf!=null) {
                Map conf = (Map) JSONValue.parse(jsonConf);
                rt = (ResourceType)conf.get(Config.STORM_COMPONENT_RESOURCE_TYPE);
            }
			
			int exec = common.get_parallelism_hint();
			totalExecutors += exec;
			switch(rt){
			case CPU: cpuExecutors+=exec;break;
			case MEMORY: memExecutors+=exec;break;
			case DISK: diskExecutors+=exec;break;
			case NETWORK: networkExecutors+=exec;break;
			case DEFAULT: commonExecutors+=exec;break;
			}
		}
		
		TopologyResource tr = new TopologyResource();
		int totalWorkers = topology.getNumWorkers();
		tr.setTotalWorkers(totalWorkers);
		tr.setCpuWorkers((cpuExecutors * totalWorkers)/totalExecutors);
		tr.setMemWorkers((memExecutors * totalWorkers)/totalExecutors);
		tr.setDiskWorkers((diskExecutors * totalWorkers)/totalExecutors);
		tr.setNetWorkers((networkExecutors * totalWorkers)/totalExecutors);
		int commonWorkers = totalWorkers -tr.getCpuWorkers() - tr.getMemWorkers()
				- tr.getDiskWorkers() - tr.getNetWorkers(); 
		tr.setCommonWorkers(commonWorkers);
		
		return tr;
	}
}
