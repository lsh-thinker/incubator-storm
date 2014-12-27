package edu.sjtu.se.dclab;

public enum ResourceType {
	
	MEMORY("MEMORY"),DISK("DISK"),CPU("CPU"),NETWORK("NETWORK"),DEFAULT("DEFAULT");
	
	private String name;
	
	private ResourceType(String name){
		this.name = name;
	}
	
	@Override
	public String toString(){
		return name;
	}
	
	
}
