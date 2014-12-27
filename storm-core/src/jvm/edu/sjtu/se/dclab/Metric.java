package edu.sjtu.se.dclab;

public  class Metric {
	
	private String val;
	private String name;
	private String type;
	private String units;
	
	private String group;
	private String desc;
	private String title;
	
	public Metric(){
		name = "";
		val = "";
		type = "";
		units = "";
	}
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}



	public String getUnits() {
		return units;
	}

	public void setUnits(String units) {
		this.units = units;
	}

	public String getVal() {
		return val;
	}



	public void setVal(String val) {
		this.val = val;
	}



	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(getName()).append("=").append(getVal()).append(getUnits()).append("(").append(type).append("),")
			.append("group=").append(getGroup())
			.append(",title=").append(getTitle())
			.append(",desc=").append(getDesc());
		return sb.toString();
	}
}


