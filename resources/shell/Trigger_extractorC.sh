#export LD_LIBRARY_PATH="/opt/oraapp/instantclient/pretend_12.1.0.2_x64_DBAocl024/lib"
export LD_LIBRARY_PATH="/opt/oraapp/instantclient/12.1.0.2_x64_DBAocl024"

userid=$1
sqlstmt=$2
arraysize=$3
delimiter=$4
enclosure=$5

/data/juniper/jar/extractorC "$userid" "$sqlstmt" "$arraysize" "$delimiter"
