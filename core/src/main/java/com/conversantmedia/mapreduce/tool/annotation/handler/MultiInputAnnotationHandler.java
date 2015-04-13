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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.DelegatingMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.Input;
import com.conversantmedia.mapreduce.tool.annotation.MultiInput;

/**
 * Configures job with the @MultiInput annotation.
 *
 * <code>
 * {@literal @}MultiInput({
 *		{@literal @}Input(mapper=BuildDeleteMapper.class),
 *		{@literal @}Input(path="${context.previous}", mapper=BuildDeletePrevMapper.class),
 *	})
 * </code>
 *
 */
@Service
public class MultiInputAnnotationHandler extends AnnotationHandlerBase {

	@Override
	public boolean accept(Annotation annotation) throws ToolException {
		return annotation.annotationType() == MultiInput.class;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target)
			throws ToolException {
		for (Input input : ((MultiInput)annotation).value()) {
			Path path = getInputAsPath(input.path());
			if (input.mapper() == Mapper.class) {
				MultipleInputs.addInputPath(job, path, input.format());
			}
			else {
				MultipleInputs.addInputPath(job, path, input.format(), input.mapper());
				// Need to call again here so the call is captured by our aspect which
				// will replace it with the annotated delegating mapper class for resource
				// injection if required.
				job.setMapperClass(DelegatingMapper.class);
			}
		}
	}

	/**
	 *
	 * @param expressionOrValue	the path or expression that when evaluated produces
	 * 				a path to be used as the input.
	 * @return					the evaluated path
	 * @throws ToolException	if the expression evaluation fails or the result isn't
	 * 				a legal path type of String or Path.
	 */
	protected Path getInputAsPath(String expressionOrValue) throws ToolException {
		Object value = this.evaluateExpression(expressionOrValue);
		if (value == null) {
			throw new ToolException("Input path property is null.");
		}

		Path path = null;
		if (value instanceof Path) {
			path = (Path)value;
		}
		else if (value instanceof String){
			path = new Path((String)value);
		}
		else {
			throw new ToolException("Illegal Path property. Path must be one of type [" + Path.class
					+ " or java.lang.String]");
		}
		return path;
	}

}
