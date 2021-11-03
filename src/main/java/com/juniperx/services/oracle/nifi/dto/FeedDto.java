package com.juniperx.services.oracle.nifi.dto;

public class FeedDto {

	private String juniper_user;
	private int feed_id;
	private String feed_name;
	private String country_code;
	private String feed_desc;
	private String feed_extract_type;
	private int src_conn_id;
	private String target;
	private String tableList;
	private String fileList;
	private String dbList;
	private String filePath;
	private String encryptionStatus;
	private String project;
	private int project_sequence;
	private String datatype_casting_clause;
	private String ts_bi_flg;
	private String file_format;
	private String compressionType;

	public String getCompressionType() {
		return compressionType;
	}

	public void setCompressionType(String compressionType) {
		this.compressionType = compressionType;
	}
	
	public String getFile_format() {
		return file_format;
	}

	public void setFile_format(String file_format) {
		this.file_format = file_format;
	}

	public String getTs_bi_flg() {
		return ts_bi_flg;
	}

	public void setTs_bi_flg(String ts_bi_flg) {
		this.ts_bi_flg = ts_bi_flg;
	}

	public String getDatatype_casting_clause() {
		return datatype_casting_clause;
	}

	public void setDatatype_casting_clause(String datatype_casting_clause) {
		this.datatype_casting_clause = datatype_casting_clause;
	}

	public String getDbList() {
		return dbList;
	}

	public void setDbList(String dbList) {
		this.dbList = dbList;
	}

	public int getProject_sequence() {
		return project_sequence;
	}

	public void setProject_sequence(int project_sequence) {
		this.project_sequence = project_sequence;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public int getFeed_id() {
		return feed_id;
	}

	public void setFeed_id(int feed_id) {
		this.feed_id = feed_id;
	}

	public String getFeed_name() {
		return feed_name;
	}

	public void setFeed_name(String feed_name) {
		this.feed_name = feed_name;
	}

	public String getFeed_desc() {
		return feed_desc;
	}

	public void setFeed_desc(String feed_desc) {
		this.feed_desc = feed_desc;
	}

	public String getFeed_extract_type() {
		return feed_extract_type;
	}

	public void setFeed_extract_type(String feed_extract_type) {
		this.feed_extract_type = feed_extract_type;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getFileList() {
		return fileList;
	}

	public void setFileList(String fileList) {
		this.fileList = fileList;
	}

	public String getTableList() {
		return tableList;
	}

	public void setTableList(String tableList) {
		this.tableList = tableList;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public String getJuniper_user() {
		return juniper_user;
	}

	public void setJuniper_user(String juniper_user) {
		this.juniper_user = juniper_user;
	}

	public String getCountry_code() {
		return country_code;
	}

	public void setCountry_code(String country_code) {
		this.country_code = country_code;
	}

	public int getSrc_conn_id() {
		return src_conn_id;
	}

	public void setSrc_conn_id(int src_conn_id) {
		this.src_conn_id = src_conn_id;
	}

	public String getEncryptionStatus() {
		return encryptionStatus;
	}

	public void setEncryptionStatus(String encryptionStatus) {
		this.encryptionStatus = encryptionStatus;
	}
}