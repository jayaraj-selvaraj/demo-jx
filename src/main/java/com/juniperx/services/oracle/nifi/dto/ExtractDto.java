package com.juniperx.services.oracle.nifi.dto;

import java.util.ArrayList;

public class ExtractDto {
	
	ConnectionDto connDto;
	FeedDto feedDto;
	TableInfoDto tableInfoDto;
	ArrayList<TargetDto> targetArr;
	
	public ArrayList<TargetDto> getTargetArr() {
		return targetArr;
	}
	public void setTargetArr(ArrayList<TargetDto> targetArr) {
		this.targetArr = targetArr;
	}
	public ConnectionDto getConnDto() {
		return connDto;
	}
	public void setConnDto(ConnectionDto connDto) {
		this.connDto = connDto;
	}	
	public FeedDto getFeedDto() {
		return feedDto;
	}
	public void setFeedDto(FeedDto feedDto) {
		this.feedDto = feedDto;
	}
	public TableInfoDto getTableInfoDto() {
		return tableInfoDto;
	}
	public void setTableInfoDto(TableInfoDto tableInfoDto) {
		this.tableInfoDto = tableInfoDto;
	}
	
}