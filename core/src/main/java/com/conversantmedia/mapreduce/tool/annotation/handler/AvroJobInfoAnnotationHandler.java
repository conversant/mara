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


import java.lang.annotation.Annotation;

import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.AvroDefault;
import com.conversantmedia.mapreduce.tool.annotation.AvroJobInfo;

/**
 *
 *
 */
@Service
public class AvroJobInfoAnnotationHandler extends AvroAnnotationHandlerBase {

	@Override
	public boolean accept(Annotation annotation) throws ToolException {
		return annotation.annotationType() == AvroJobInfo.class;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target)
			throws ToolException {

		AvroJobInfo avroInfo = (AvroJobInfo)annotation;
		if (avroInfo.inputKeySchema() != AvroDefault.class) {
			AvroJob.setInputKeySchema(job, getSchema(avroInfo.inputKeySchema()));
		}
		if (avroInfo.inputValueSchema() != AvroDefault.class) {
			AvroJob.setInputValueSchema(job, getSchema(avroInfo.inputValueSchema()));
		}

		if (avroInfo.outputKeySchema() != AvroDefault.class) {
			AvroJob.setOutputKeySchema(job, getSchema(avroInfo.outputKeySchema()));
		}
		if (avroInfo.outputValueSchema() != AvroDefault.class) {
			AvroJob.setOutputValueSchema(job, getSchema(avroInfo.outputValueSchema()));
		}

		if (avroInfo.mapOutputKeySchema() != AvroDefault.class) {
			AvroJob.setMapOutputKeySchema(job, getSchema(avroInfo.mapOutputKeySchema()));
		}
		if (avroInfo.mapOutputValueSchema() != AvroDefault.class) {
			AvroJob.setMapOutputValueSchema(job, getSchema(avroInfo.mapOutputValueSchema()));
		}

		AvroSerialization.addToConfiguration(job.getConfiguration());
	}

}
