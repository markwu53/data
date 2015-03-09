package com.nationwide.data;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class CreateTables {

        private static String schema;
        private static String netflowTableName;

        private static final String header = "Flow Type,First Packet Time,Storage Time,Source IP,Source Port,Destination IP,Destination Port,Source Bytes,Destination Bytes,Total Bytes,Source Packets,Destination Packets,Total Packets,Protocol,Application,ICMP Type/Code,Source Flags,Destination Flags,Source QoS,Destination QoS,Flow Source,Flow Interface,Source If Index,Destination If Index,Source ASN,Destination ASN";

        public static void main(String[] args) throws FileNotFoundException, IOException {
                go1();
        }

        public static void go1() throws FileNotFoundException, IOException {
                Properties properties = new Properties();
                properties.load(new FileInputStream("logdata.properties"));
                schema = properties.getProperty("schema");
                netflowTableName = properties.getProperty("netflowTableName");

                String[] splits = header.split(",");
                String columnDef = "";
                int count = 0;
                for (String split: splits) {
                        columnDef += "\t";
                        if (count != 0) {
                                columnDef += ", ";
                        }
                        columnDef += split.replaceAll("\\s+", "_").replaceAll("/", "_");
                        columnDef += " string \n";
                        count ++;
                }
                
                String partition = "partitioned by (App string, Path string) ";
                String rowFormat = "row format serde 'com.bizo.hive.serde.csv.CSVSerde'";

                String sql = String.format("\ncreate table %s.%s (\n%s)\n%s\n%s\n;\n",
                                schema, netflowTableName, columnDef, partition, rowFormat);
                
                System.out.println(sql);
        }

}
