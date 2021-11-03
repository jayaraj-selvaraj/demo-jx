package com.juniperx.services.oracle.nifi.constant;

public class NifiConstants {

    public static final  String DISABLECONTROLLERCOMMAND =
        "{\"revision\":{\"clientId\":\"${clientid}\",\"version\":${version}},\"component\":{\"id\":\"${id}\",\"state\":\"DISABLED\"}}";
    public static final String PROCESSORURL = "nifi-api/processors/${id}";
    public static final String STOPPROCESSOR = "{\n" + "  \"status\": {\n" + "    \"runStatus\": \"STOPPED\"\n" + "  },\n"
        + "  \"component\": {\n" + "    \"state\": \"STOPPED\",\n" + "    \"id\": \"${id}\"\n" + "  },\n" + "  \"id\": \"${id}\",\n"
        + "  \"revision\": {\n" + "    \"version\": ${version},\n" + "    \"clientId\": \"${clientId}\"\n" + "  }\n" + "}";
    public static final  String STARTPROCESSOR = "{\n" + "  \"status\": {\n" + "    \"runStatus\": \"RUNNING\"\n" + "  },\n"
        + "  \"component\": {\n" + "    \"state\": \"RUNNING\",\n" + "    \"id\": \"${id}\"\n" + "  },\n" + "  \"id\": \"${id}\",\n"
        + "  \"revision\": {\n" + "    \"version\": ${version},\n" + "    \"clientId\": \"${clientId}\"\n" + "  }\n" + "}";

    public static final  String STOPPROCESSOR2 = "{\n" + "  \"status\": {\n" + "    \"runStatus\": \"STOPPED\"\n" + "  },\n"
        + "  \"component\": {\n" + "    \"state\": \"STOPPED\",\n" + "    \"id\": \"${id}\"\n" + "  },\n" + "  \"id\": \"${id}\",\n"
        + "  \"revision\": {\n" + "    \"version\": ${version},\n" + "   \"clientId\": \"\" \n" + "  }\n" + "}";

    public static final String UPDATEDBCONNECTIONPOOL =
        "{\"revision\":{\"clientId\":\"${clientId}\",\"version\":${ver}},\"component\":{\"id\":\"${contId}\",\"state\":\"DISABLED\", \"properties\":{\"Database Connection URL\":\"${conUrl}\",\"Database User\":\"${user}\",\"Password\":\"${pasword}\"}}}";
    
    public static final String AVROCONTROLLER =
            "{\"revision\":{\"clientId\":\"${clientId}\",\"version\":${ver}},\"component\":{\"id\":\"${contId}\",\"state\":\"DISABLED\", \"properties\":{\"compression-format\":\"${compFormat}\"}}}";
    
    public static final String ENABLEDBCONNECTIONPOOL =
        "{\"revision\":{\"clientId\":\"${clientId}\",\"version\":${ver}},\"component\":{\"id\":\"${contId}\",\"state\":\"ENABLED\"}}";

    public static final String UPDATEEXECUTESQL =
        "{\n" + "  \"status\": {\n" + "    \"runStatus\": \"STOPPED\"\n" + "  },\n" + "  \"component\": {\n"
            + "    \"state\": \"STOPPED\",\n" + "    \"id\": \"${id}\",\n" + "   \"config\": {\n" + "   \"properties\": {\n"
            + "   \"SQL select query\" : \"${query}\"\n" + "  }\n" + "  }\n" + "  },\n" + "  \"id\": \"${id}\",\n"
            + "  \"revision\": {\n" + "    \"version\": ${version},\n" + "   \"clientId\": \"${clientId}\" \n" + "  }\n" + "}";

    public static final String QUERY =
        "select feed_unique_name as feed_name, run_id as run_id,job_name as job_name ,status as status from JUNIPER_EXT_NIFI_STATUS where feed_unique_name='${feed_name}' and run_id='${run_id}' and nifi_pg= ${index} and upper(job_name)='${job_name}'";
    public static final String PROCESSGROUPURL = "nifi-api/flow/process-groups/${id}";
    public static final String STARTPROCESSGROUP="{\"id\":\"${id}\",\"state\":\"RUNNING\"}";
	public static final String STOPPROCESSGROUP = "{\"id\":\"${id}\",\"state\":\"STOPPED\"}";

}