package edu.sjtu.se.dclab;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostMetric {
	
    private static final Logger LOG = LoggerFactory.getLogger(HostMetric.class);
	
	private String hostname;
	private String ipaddr;
	
	Map<String, Metric> metricMap;
	
	
	public HostMetric(){
		metricMap = new HashMap<String, Metric>();
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getIpaddr() {
		return ipaddr;
	}

	public void setIpaddr(String ipaddr) {
		this.ipaddr = ipaddr;
	}

	public Map<String, Metric> getMetricMap() {
		return metricMap;
	}
	
	public void print(){
		LOG.info("hostname = " + hostname);
		LOG.info("ipaddr=" + ipaddr);
		for(Map.Entry<String, Metric> entry: metricMap.entrySet()){
			LOG.info(entry.getValue().toString());
		}
	}
	
	public String getMetricValue(String metricName){
		if (metricMap.containsKey(metricName)){
			return metricMap.get(metricName).getVal();
		}
		return null;
	}
	
	
}
