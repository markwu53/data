package com.nationwide.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class LoadDataLocal {

        private static String baseDir;
        private static String schema;
        private static String netflowTableName;
        private static String ihsTableName;
        private static String netflowDir;
        private static String ihsDir;

        public static void main(String[] args) throws IOException {

                Properties properties = new Properties();
                properties.load(new FileInputStream("logdata.properties"));
                baseDir = properties.getProperty("baseDir");
                schema = properties.getProperty("schema");
                netflowTableName = properties.getProperty("netflowTableName");
                ihsTableName = properties.getProperty("ihsTableName");
                netflowDir = properties.getProperty("netflowDir");
                ihsDir = properties.getProperty("ihsDir");

                loadLogs();

        }

        public static void loadLogs() {
                File base = new File(baseDir);
                for (File app: base.listFiles()) {
                        if (!app.isDirectory()) {
                                continue;
                        }
                        for (File logType: app.listFiles()) {
                                if (!logType.isDirectory()) {
                                        continue;
                                }
                                if (!logType.getName().equals(ihsDir) && !logType.getName().equals(netflowDir)) {
                                        continue;
                                }
                                String tableName = logType.getName().equals(ihsDir)? ihsTableName : netflowTableName;
                                for (File file: logType.listFiles()) {
                                        if (file.isFile()) {
                                                String path = file.getName();
                                                System.out.println(String.format("load data local inpath '%s/%s/%s/%s' into table %s.%s partition (App='%s', Path='%s');",
                                                                baseDir, app.getName(), logType.getName(), path, schema, tableName, app.getName(), path));
                                        } else if (file.isDirectory()) {
                                                for (File f: file.listFiles()) {
                                                        if (!f.isFile()) {
                                                                continue;
                                                        }
                                                        String path = String.format("%s/%s", file.getName(), f.getName());
                                                        System.out.println(String.format("load data local inpath '%s/%s/%s/%s' into table %s.%s partition (App='%s', Path='%s');",
                                                                        baseDir, app.getName(), logType.getName(), path, schema, tableName, app.getName(), path));
                                                }
                                        }
                                }
                        }
                }
        }

}
