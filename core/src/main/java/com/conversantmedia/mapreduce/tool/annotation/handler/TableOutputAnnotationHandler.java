package com.conversantmedia.mapreduce.tool.annotation.handler;

/*
 * #%L
 * Mara Core framework
 * ~~
 * Copyright (C) 2015 Conversant
 * ~~
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import java.io.IOException;
import java.lang.annotation.Annotation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.TableOutput;

@Service
public class TableOutputAnnotationHandler extends AnnotationHandlerBase {

	@Override
	public boolean accept(Annotation annotation) throws ToolException {
		return annotation.annotationType() == TableOutput.class;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target)
			throws ToolException {

		TableOutput tableOutput = (TableOutput)annotation;

		// Base setup of the table job
		Configuration conf = job.getConfiguration();
		HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

		// Add dependencies
		try {
			TableMapReduceUtil.addDependencyJars(job);
		} catch (IOException e) {
			throw new ToolException(e);
		}

		// Set table output format
		job.setOutputFormatClass(TableOutputFormat.class);

		// Set the table name
		String tableName = (String)this.evaluateExpression(tableOutput.value());
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);

	}

}
