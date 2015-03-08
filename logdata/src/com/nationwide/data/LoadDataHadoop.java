package com.nationwide.data;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LoadDataHadoop {

        private static String hadoopConfDir;
        private static String hdfsBaseDir;
        private static String schema;
        private static String netflowTableName;
        private static String ihsTableName;
        private static String netflowDir;
        private static String ihsDir;

        private static FileSystem hdfs;

        private static List<String> statements = new ArrayList<String>();

        public static void main(String[] args) throws IOException {

                Properties properties = new Properties();
                properties.load(new FileInputStream("logdata.properties"));
                hadoopConfDir = properties.getProperty("hadoopConfDir");
                hdfsBaseDir = properties.getProperty("hdfsBaseDir");
                schema = properties.getProperty("schema");
                netflowTableName = properties.getProperty("netflowTableName");
                ihsTableName = properties.getProperty("ihsTableName");
                netflowDir = properties.getProperty("netflowDir");
                ihsDir = properties.getProperty("ihsDir");

                Configuration conf = new Configuration();
                conf.addResource(new Path(hadoopConfDir + "core-site.xml"));
                conf.addResource(new Path(hadoopConfDir + "hdfs-site.xml"));
                conf.addResource(new Path(hadoopConfDir + "mapred-site.xml"));
                hdfs = FileSystem.get(conf);

                loadDataStatements();

                for (String sql: statements) {
                        System.out.println(sql);
                }
        }

        public static void loadDataStatements() throws IOException {
                int index = hdfsBaseDir.indexOf("//");
                index = hdfsBaseDir.indexOf("/", index + 2);
                String hdfsDir = hdfsBaseDir.substring(index);
                Path base = new Path(hdfsBaseDir);
                for (FileStatus app: hdfs.listStatus(base)) {
                        String appName = app.getPath().getName();
                        //System.out.println(appName);
                        for (FileStatus type: hdfs.listStatus(app.getPath())) {
                                String typeName = type.getPath().getName();
                                //System.out.println(typeName);
                                if (typeName.equals(netflowDir)) {
                                        for (FileStatus file: hdfs.listStatus(type.getPath())) {
                                                String filename = file.getPath().getName();
                                                statements.add(String.format("load data inpath '%s%s/Netflow/%s' into table %s.%s partition (App='%s', Path='%s');",
                                                                hdfsDir, appName, filename, schema, netflowTableName, appName, filename));
                                        }
                                } else if (typeName.equals(ihsDir)) {
                                        for (FileStatus file: hdfs.listStatus(type.getPath())) {
                                                if (file.isFile()) {
                                                        String filename = file.getPath().getName();
                                                        statements.add(String.format("load data inpath '%s%s/IHS_Logs/%s' into table %s.%s partition (App='%s', Path='%s');",
                                                                        hdfsDir, appName, filename, schema, ihsTableName, appName, filename));
                                                } else if (file.isDirectory()){
                                                        String subdir = file.getPath().getName();
                                                        for (FileStatus subfile: hdfs.listStatus(file.getPath())) {
                                                                String filename = subfile.getPath().getName();
                                                                String path = String.format("%s/%s", subdir, filename);
                                                                statements.add(String.format("load data inpath '%s%s/IHS_Logs/%s/%s' into table %s.%s partition (App='%s', Path='%s');",
                                                                                hdfsDir, appName, subdir, filename, schema, ihsTableName, appName, path));
                                                        }
                                                }
                                        }
                                }
                        }
                }
        }
}
