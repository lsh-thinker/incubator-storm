package edu.sjtu.se.dclab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GangliaClient {
	
    private static final Logger LOG = LoggerFactory.getLogger(GangliaClient.class);

	private static final String COMMAND = "telnet localhost 8651"; 
	
	public String getGangliaMetricString() {
		Runtime rt = Runtime.getRuntime();
		StringBuffer metricString = new StringBuffer();
		BufferedReader in = null;
		Process p;
		try {
			p = rt.exec(COMMAND);
			in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = null;
			boolean isValid = false;
			int skip = 3;
			while((line = in.readLine())!= null){
				if (isValid){
					LOG.info(line);
					metricString.append(line);
					continue;
				}
				--skip;
				if (skip == 0) isValid = true;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return metricString.toString();
	}
	
	public GangliaCluster getGangliaMetric(){
		String metricString = getGangliaMetricString();
		if (metricString == null || metricString.equals("")) return null;
		MetricXMLParser parser = new MetricXMLParser(metricString);
		return parser.parse();
	}

	public static void main(String[] args) {
		GangliaCluster cluster = new GangliaClient().getGangliaMetric();
		cluster.print();
	}

}
