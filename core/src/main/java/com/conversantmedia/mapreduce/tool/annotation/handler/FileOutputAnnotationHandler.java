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
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.FileOutput;

@Service
public class FileOutputAnnotationHandler extends AnnotationHandlerBase {

	@Override
	public boolean accept(Annotation annotation) {
		return annotation.annotationType() == FileOutput.class;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target) throws ToolException {
		try {
			configureOutputs(job, (FileOutput)annotation);
		} catch (Exception e) {
			throw new ToolException(e);
		}
	}

	protected void configureOutputs(Job job, FileOutput fileOutput)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException, ToolException, IllegalArgumentException, IOException {
		job.setOutputFormatClass(fileOutput.value());
		// The property used for retrieving the path
		Object path = this.evaluateExpression(fileOutput.path());
		configureFileOutputPaths(job, path);
	}

	private void configureFileOutputPaths(Job job, Object value)
			throws IllegalArgumentException, IllegalAccessException, IOException, ToolException {
		if (value == null) {
			throw new ToolException("Output path property is null.");
		}
		if (value instanceof Path) {
			FileOutputFormat.setOutputPath(job, (Path)value);
		}
		else if (value instanceof String){
			FileOutputFormat.setOutputPath(job, new Path((String)value));
		}
		else {
			throw new ToolException("Illegal Path property. Path must be one of type [" + Path.class
					+ ", " + Path[].class + ", or java.lang.String]");
		}
	}
}
