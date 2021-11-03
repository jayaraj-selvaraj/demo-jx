package com.juniperx.services.oracle.nifi.dto;

import java.util.ArrayList;

public class TableInfoDto {
	
	String juniper_user;
	int feed_id;
	ArrayList<TableMetadataDto> tableMetadataArr;
	String project;
	String incr_flag="";
	
	public String getIncr_flag() {
		return incr_flag;
	}
	public void setIncr_flag(String incr_flag) {
		this.incr_flag = incr_flag;
	}
	public String getProject() {
		return project;
	}
	public void setProject(String project) {
		this.project = project;
	}
	public String getJuniper_user() {
		return juniper_user;
	}
	public void setJuniper_user(String juniper_user) {
		this.juniper_user = juniper_user;
	}
	public int getFeed_id() {
		return feed_id;
	}
	public void setFeed_id(int feed_id) {
		this.feed_id = feed_id;
	}
	public ArrayList<TableMetadataDto> getTableMetadataArr() {
		return tableMetadataArr;
	}
	public void setTableMetadataArr(ArrayList<TableMetadataDto> tableMetadataArr) {
		this.tableMetadataArr = tableMetadataArr;
	}
	
}