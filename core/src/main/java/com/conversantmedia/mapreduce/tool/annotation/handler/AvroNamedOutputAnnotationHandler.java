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
import java.util.HashSet;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.AvroNamedOutput;

/**
 * Sets up a Avro multiple output.
 *
 */
@Service
public class AvroNamedOutputAnnotationHandler extends AvroAnnotationHandlerBase {

	private Set<String> configured = new HashSet<String>();

	@Override
	public boolean accept(Annotation annotation) throws ToolException {
		return annotation.annotationType() == AvroNamedOutput.class;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target)
			throws ToolException {
		AvroNamedOutput avroOut = (AvroNamedOutput)annotation;

		Schema schema = getSchema(avroOut.record());
		String[] names = getNames(avroOut);
		for (String name : names) {
			name = (String)evaluateExpression(name);
			if (!configured.contains(name)) {
				AvroMultipleOutputs.addNamedOutput(job, name, avroOut.format(), schema);
				AvroMultipleOutputs.setCountersEnabled(job, avroOut.countersEnabled());
				configured.add(name);
			}
		}

		AvroSerialization.addToConfiguration(job.getConfiguration());
	}

	public String[] getNames(AvroNamedOutput namedOut) {
		String[] names = new String[1];
		if (namedOut.value().length > 1) {
			// Must be specified as @AvroNamedOutput({"name1","name2"})
			names = namedOut.value();
		}
		else if (namedOut.name().length > 1) {
			// Must be specified as @AvroNamedOutput(name={"name1","name2"},..)
			names = namedOut.name();
		}
		else {
			names[0] = namedOut.value()[0].equals("default")? namedOut.name()[0] : namedOut.value()[0];
		}
		return names;
	}
}
