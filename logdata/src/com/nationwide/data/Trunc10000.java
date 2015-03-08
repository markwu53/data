package com.nationwide.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


public class Trunc10000 {

        private static final String sourceDir = "/mnt/hgfs/Downloads/IRM_POC1/Files";
        private static final String targetDir = "/home/biadmin/Files10000";
        private static final int total = 10000;
 
        public static void main(String[] args) throws IOException {
                trunc(new File(sourceDir), new File(targetDir));
        }

        private static void trunc(File source, File target) throws IOException {
                if (source.isFile()) {
                        target.createNewFile();
                        truncFile(source, target);
                        return;
                }
                target.mkdir();
                for (File subSource: source.listFiles()) {
                        trunc(subSource, new File(String.format("%s/%s", target.getPath(), subSource.getName())));
                }
        }

        private static void truncFile(File source, File target) throws IOException {
                System.out.println(String.format("writing %s/%s", target.getPath(), target.getName()));
                BufferedReader in = new BufferedReader(new FileReader(source));
                PrintWriter out = new PrintWriter(new FileWriter(target));
                for (int count = 0; count < total; count ++) {
                        String line = in.readLine();
                        if (line == null) {
                                break;
                        }
                        out.println(line);
                }
                out.close();
                in.close();
        }
}
