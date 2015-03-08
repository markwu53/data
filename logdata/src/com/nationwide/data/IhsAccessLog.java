package com.nationwide.data;

import java.io.File;

public class IhsAccessLog {

        private static final String localBaseDir = "/Users/wum2/Programs/SRE/Files10000/";

        public static void main(String[] args) {
                go();
        }
        
        private static void go() {
                File baseDir = new File(localBaseDir);
                for (File app: baseDir.listFiles()) {
                        if (app.isDirectory()) {
                                String appName = app.getName();
                                for (File logType: app.listFiles()) {
                                        if (!logType.isDirectory()) {
                                                continue;
                                        }
                                        if (logType.getName().equals("IHS_Logs")) {
                                                continue;
                                        }
                                        for (File file: logType.listFiles()) {
                                                if (file.isFile()) {
                                                        String path = file.getName();
                                                        processFile(file);
                                                } else {
                                                        for (File f: file.listFiles()) {
                                                                String path = file.getName() + "_slash_" + f.getName();
                                                                processFile(f);
                                                        }
                                                }
                                        }
                                }
                        }
                }
        }
        
        private static void processFile(File file) {
        }

}
