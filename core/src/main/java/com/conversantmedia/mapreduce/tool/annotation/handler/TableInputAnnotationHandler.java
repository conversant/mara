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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.TableInput;

@Service
public class TableInputAnnotationHandler extends AnnotationHandlerBase {

	@Override
	public boolean accept(Annotation annotation) throws ToolException {
		return annotation.annotationType() == TableInput.class;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target)
			throws ToolException {

		TableInput tableInput = (TableInput)annotation;

		// Base setup of the table mapper job
		Configuration conf = job.getConfiguration();
		HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));

		try {
			// Add dependencies
			TableMapReduceUtil.addDependencyJars(job);

			// Setup the input table
			Object tableNameObj = this.evaluateExpression(tableInput.table());
			String tableName;
			if (tableNameObj instanceof byte[]) {
				tableName = Bytes.toString((byte[])tableNameObj);
			}
			else {
				tableName = tableNameObj.toString();
			}

			Scan scan = new Scan();
			String scanProperty = tableInput.scanProperty();
			if (StringUtils.isNotBlank(scanProperty)) {
				if (!StringUtils.startsWith(scanProperty, "${")) {
					scanProperty = "${" + scanProperty + "}";
				}
				try {
					scan = (Scan)this.evaluateExpression(scanProperty);
				} catch (Exception e) { // ignore and use default
				}
			}

			job.setInputFormatClass(TableInputFormat.class);
			conf.set(TableInputFormat.INPUT_TABLE, tableName);
			conf.set(TableInputFormat.SCAN, convertScanToString(scan));

		} catch (IOException e) {
			throw new ToolException(e);
		}
	}

	/**
	 * COPIED from {@link TableMapReduceUtil} since it's not visible in that class.
	 * Makes this class somewhat brittle, but if we call the static method directly we
	 * risk overwriting the mapper class.
	 * 
	 * @param scan			the scan to convert
	 * @return				string-representation of the scan
	 * @throws IOException	if encoding to base64 bytes fails
	 */
	protected String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
	    return Base64.encodeBytes(proto.toByteArray());
	}
}
