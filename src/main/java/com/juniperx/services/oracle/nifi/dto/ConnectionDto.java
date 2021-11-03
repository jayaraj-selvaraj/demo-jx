package com.juniperx.services.oracle.nifi.dto;

public class ConnectionDto {
	
	String conn_name;
	String conn_type;
	String hostName;
	String port;
	String userName;
	String password;
	int credentialId;
	String dbName;
	String serviceName;
	int connId;
	String system;
	String project;
	String juniper_user;
	int sys_seq;
	int proj_seq;
	String connection_string;
	
	public int getCredentialId() {
		return credentialId;
	}

	public void setCredentialId(int credentialId) {
		this.credentialId = credentialId;
	}

	public String getConnection_string() {
		return connection_string;
	}

	public void setConnection_string(String connection_string) {
		this.connection_string = connection_string;
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
	public int getSys_seq() {
		return sys_seq;
	}
	public void setSys_seq(int sys_seq) {
		this.sys_seq = sys_seq;
	}
	public int getProj_seq() {
		return proj_seq;
	}
	public void setProj_seq(int proj_seq) {
		this.proj_seq = proj_seq;
	}
	
	public int getConnId() {
		return connId;
	}
	public void setConnId(int connId) {
		this.connId = connId;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	public String getServiceName() {
		return serviceName;
	}
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getConn_name() {
		return conn_name;
	}
	public void setConn_name(String conn_name) {
		this.conn_name = conn_name;
	}
	public String getConn_type() {
		return conn_type;
	}
	public void setConn_type(String conn_type) {
		this.conn_type = conn_type;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getSystem() {
		return system;
	}
	public void setSystem(String system) {
		this.system = system;
	}

}