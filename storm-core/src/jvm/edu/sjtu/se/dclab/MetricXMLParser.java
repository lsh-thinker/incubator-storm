package edu.sjtu.se.dclab;

import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricXMLParser {
	
    private static final Logger LOG = LoggerFactory.getLogger(MetricXMLParser.class);
	
	private GangliaCluster cluster = null;
	
	public MetricXMLParser(){
	}
	
	private Document document;

	public MetricXMLParser(String metricString) {
		try {
			System.out.println(metricString);
			document = DocumentHelper.parseText(metricString);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}

	public GangliaCluster parse(){
		if(document == null) return null;
		cluster = new GangliaCluster();
		Element rootElement = document.getRootElement();
		Element gridElement = rootElement.element(MetricConst.L_GRID);
		parseCluster(gridElement.element(MetricConst.L_CLUSTER));
		return cluster;
	}
	
	private void parseCluster(Element clusterElement){
		
		cluster.setName( clusterElement.attributeValue( MetricConst.A_NAME ) );
		cluster.setName( clusterElement.attributeValue( MetricConst.A_NAME ) );
		
		@SuppressWarnings("unchecked")
		List<Element> hostElements = clusterElement.elements(MetricConst.L_HOST);
		for(Element element: hostElements){
			parseHost(element);
		}
		
	}

	private void parseHost(Element hostELement){
		LOG.info("Parse HOST :" + hostELement.attributeValue(MetricConst.A_NAME));
		LOG.info("Parse IP :" + hostELement.attributeValue(MetricConst.A_IP));
		HostMetric hostMetric = new HostMetric(); 
		@SuppressWarnings("unchecked")
		List<Element> metricElements = hostELement.elements(MetricConst.L_METRIC);
		for(Element element: metricElements){
			Metric m = parseMetric(element);
			hostMetric.metricMap.put(m.getName(), m);
		}
		hostMetric.setHostname(hostELement.attributeValue(MetricConst.A_NAME));
		hostMetric.setIpaddr(hostELement.attributeValue(MetricConst.A_IP));
		cluster.nameToHost.put(hostMetric.getHostname(), hostMetric);
	}
	
	private Metric parseMetric(Element metricElement){
		Metric m = new Metric();
		m.setName( metricElement.attributeValue( MetricConst.A_NAME));
		m.setType( metricElement.attributeValue( MetricConst.A_TYPE));
		m.setVal( metricElement.attributeValue( MetricConst.A_VAL));
		m.setUnits( metricElement.attributeValue( MetricConst.A_UNITS));
		
		Element extraDataElement = metricElement.element(MetricConst.L_EXTRA_DATA);
		@SuppressWarnings("unchecked")
		List<Element> extraElements = extraDataElement.elements(MetricConst.L_EXTRA_ELEMENT);
		for(Element element: extraElements){
			if (element.attributeValue(MetricConst.A_NAME).equals(MetricConst.EXTRA_NAME_GROUP)){
				m.setGroup(element.attributeValue(MetricConst.A_VAL));
				continue;
			}
			if (element.attributeValue(MetricConst.A_NAME).equals(MetricConst.EXTRA_NAME_DESC)){
				m.setDesc(element.attributeValue(MetricConst.A_VAL));
				continue;
			}
			if (element.attributeValue(MetricConst.A_NAME).equals(MetricConst.EXTRA_NAME_TITLE)){
				m.setTitle(element.attributeValue(MetricConst.A_VAL));
				continue;
			}
		}
//		LOG.info(m.toString());
		return m;
	}
	
	
}
