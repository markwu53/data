package com.wu.serde;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("deprecation")
public class IhsLogSerDe implements SerDe {

        public void initialize(Configuration arg0, Properties arg1) throws SerDeException {
        }

        public Object deserialize(Writable arg) throws SerDeException {
                List<String> row = new ArrayList<String>();
                String line = arg.toString();
                int index = line.indexOf('[');
                if (index != -1) {
                        String[] splits = line.substring(0, index).split(" ");
                        if (splits.length == 4) {
                                //98.238.249.242 97188 - - [17/Jan/2015:00:02:47 -0600] "GET /center/templates/welcome.cfm HTTP/1.1" 200 8341
                                String ip = splits[0];
                                String port = splits[1].equals("-")? null : splits[1];
                                String process = splits[3].equals("-")? null : splits[3];
                                row.add(ip);
                                row.add(port);
                                row.add(process);
                        } else if (splits.length ==3) {
                                //216.34.61.62 - methodm [17/Jan/2015:00:00:00 -0500] "GET /css/standard_ie8.css HTTP/1.1" 200 1837
                                String ip = splits[0];
                                String port = null;
                                String process = splits[2].equals("-")? null : splits[2];
                                row.add(ip);
                                row.add(port);
                                row.add(process);
                        } else {
                                return null;
                        }
                        int index2 = line.indexOf(']', index);
                        if (index2 == -1) {
                                return null;
                        }
                        row.add(line.substring(index + 1, index2));
                        int quote1 = line.indexOf('"');
                        if (quote1 == -1) {
                                return null;
                        }
                        int quote2 = line.indexOf('"', quote1 + 1);
                        if (quote2 == -1) {
                                return null;
                        }
                        String[] splits2 = line.substring(quote1 + 1, quote2).split(" ");
                        if (splits2.length != 3) {
                                return null;
                        }
                        String requestMethod = splits2[0];
                        String requestPath = splits2[1];
                        String httpVersion = splits2[2];
                        row.add(requestMethod);
                        row.add(requestPath);
                        row.add(httpVersion);
                        String[] splits3 = line.substring(quote2 + 2).split(" ");
                        if (splits3.length != 2) {
                                return null;
                        }
                        String responseStatus = splits3[0].equals("-")? null : splits3[0];
                        String contentLength = splits3[1].equals("-")? null : splits3[1];
                        row.add(responseStatus);
                        row.add(contentLength);
                } else {
                        //76.89.84.198,-,-,17/Jan/2015:00:00:06 -0500,GET,/iApp/ssc/clientAccounts/retirementPlans.action?DCSext.SSC_HOME_MAIN=Retirement%20Plans,HTTP/1.1,200,69590
                        String[] splits = line.split(",");
                        if (splits.length != 9) {
                                return null;
                        }
                        row.add(splits[0]);
                        row.add(splits[1].equals("-")? null : splits[1]);
                        row.add(splits[2].equals("-")? null : splits[2]);
                        row.add(splits[3]);
                        row.add(splits[4]);
                        row.add(splits[5]);
                        row.add(splits[6]);
                        row.add(splits[7].equals("-")? null : splits[7]);
                        row.add(splits[8].equals("-")? null : splits[8]);
                }
                return row;
        }

        public ObjectInspector getObjectInspector() throws SerDeException {
                return null;
        }

        public SerDeStats getSerDeStats() {
                return null;
        }

        public Class<? extends Writable> getSerializedClass() {
                return null;
        }

        public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
                return null;
        }

}
