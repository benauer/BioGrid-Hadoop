package com.custom;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

 public class CustomCSVFormat extends MultipleTextOutputFormat<Text, Text> {

	@Override
    	protected String generateLeafFileName(String name) {
	        return name + ".csv";
    	}

 }
