package com.conversantmedia.mapreduce.tool.annotation.handler;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.MultiTableInput;
import com.conversantmedia.mapreduce.tool.annotation.TableInput;

@Service
public class MultiTableInputAnnotationHandler extends TableInputAnnotationHandler {

	@Override
	public boolean accept(Annotation annotation) throws ToolException {
		return annotation.annotationType() == MultiTableInput.class;
	}

	/*
	 * (non-Javadoc)
	 * @see com.dotomi.mapreduce.common.tool.annotation.JobAnnotationHandler#process(java.lang.annotation.Annotation, org.apache.hadoop.mapreduce.Job, java.lang.Object)
	 */
	@Override
	public void process(Annotation annotation, Job job, Object target)
			throws ToolException {

		try {
			// Base setup of the table mapper job
			Configuration conf = job.getConfiguration();
			HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
	
			List<String> scans = new ArrayList<>();
	
			MultiTableInput multiInputAnnotation = (MultiTableInput)annotation;
			for (TableInput tableInput : multiInputAnnotation.value()) {
				String tableName = getTableName(tableInput);
				Scan scan = getScan(tableInput);
				scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName.getBytes());
				scans.add(convertScanToString(scan));
			}
	
		    job.getConfiguration().setStrings(MultiTableInputFormat.SCANS, scans.toArray(new String[scans.size()]));
			job.setInputFormatClass(MultiTableInputFormat.class);

			HBaseConfiguration.addHbaseResources(job.getConfiguration());
		    TableMapReduceUtil.addDependencyJars(job);	
		}
		catch (IOException e) {
			throw new ToolException(e);
		}
	}
}
