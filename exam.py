from py4j.java_gateway import JavaGateway
import configparser

from py4j.java_gateway import GatewayServer

# Py4J GatewayServer 객체 생성
gateway_server = GatewayServer()

# Py4J GatewayServer 실행
gateway_server.start()

config = configparser.ConfigParser()
config.read('C:/Users/lgg00/project/Job_Data_Market/config.ini')

hdfs_path = config.get('path', 'hdfs_path')
local_path = config.get('path', 'local_path')

# Java Gateway 객체 생성
gateway = JavaGateway()

# HDFS API를 호출할 Java 객체 생성
fs = gateway.jvm.org.apache.hadoop.fs.FileSystem.get(gateway.jvm.java.net.URI("hdfs://localhost:9000/"), gateway.jvm.org.apache.hadoop.conf.Configuration())

# HDFS에 파일 저장하기
input_stream = gateway.jvm.java.io.FileInputStream(local_path)
output_stream = fs.create(gateway.jvm.org.apache.hadoop.fs.Path(hdfs_path))
gateway.jvm.org.apache.commons.io.IOUtils.copy(input_stream, output_stream)
