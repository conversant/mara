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
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.KeyValue;
import com.conversantmedia.mapreduce.tool.annotation.NamedOutput;

/**
 * Sets up a multiple output.
 *
 */
@Service
public class NamedOutputAnnotationHandler extends AnnotationHandlerBase {

	private final Set<String> configured = new HashSet<>();

	@Override
	public boolean accept(Annotation annotation) throws ToolException {
		return annotation.annotationType() == NamedOutput.class;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target)
			throws ToolException {
		NamedOutput namedOut = (NamedOutput)annotation;
		KeyValue kv = namedOut.type();

		// If this is a MultipleOutputs member we're annotating, see if we can't
		// get the key/value from the parameters if there are any.
		Pair<Type, Type> kvTypePair = getGenericTypeParams(target);

		Class<?> keyClass = kv.key();
		if (keyClass == void.class) {
			if (kvTypePair != null) {
				keyClass = (Class<?>)kvTypePair.getKey();
			}
			else {
				// fall back on job output key class
				keyClass = job.getOutputKeyClass();
			}
		}

		Class<?> valueClass = kv.value();
		if (valueClass == void.class) {
			if (kvTypePair != null) {
				valueClass = (Class<?>)kvTypePair.getValue();
			}
			else {
				valueClass = job.getOutputValueClass();
			}
		}

		String[] names = getNames(namedOut);
		for (String name : names) {
			name = (String)evaluateExpression(name);
			if (!configured.contains(name)) {
				MultipleOutputs.addNamedOutput(job, name, namedOut.format(), keyClass, valueClass);
				MultipleOutputs.setCountersEnabled(job, namedOut.countersEnabled());
				configured.add(name);
			}
		}
	}

	public String[] getNames(NamedOutput namedOut) {
		String[] names = new String[1];
		if (namedOut.value().length > 1) {
			// Must be specified as @NamedOutput({"name1","name2"})
			names = namedOut.value();
		}
		else if (namedOut.name().length > 1) {
			// Must be specified as @NamedOutput(name={"name1","name2"},..)
			names = namedOut.name();
		}
		else {
			names[0] = namedOut.value()[0].equals("default")? namedOut.name()[0] : namedOut.value()[0];
		}
		return names;
	}

	/**
	 * If this is a multiple output we're annotating, see if there are type parameters to
	 * use for the key/value classes.
	 * 
	 * @param target	object to reflect for type params
	 * @return			the key/value type parameters
	 */
	protected Pair<Type, Type> getGenericTypeParams(Object target) {
		Pair<Type, Type> kvTypePair = null;
		if (target instanceof Field) {
			Field field = (Field)target;
			if (field.getType() == MultipleOutputs.class) {
				Type genericType = field.getGenericType();
				if (genericType instanceof ParameterizedType) {
					Type[] keyValueTypes = ((ParameterizedType)genericType).getActualTypeArguments();
					if (keyValueTypes != null && keyValueTypes.length == 2) {
						kvTypePair = new ImmutablePair<>(keyValueTypes[0], keyValueTypes[1]);
					}
				}
			}
		}
		return kvTypePair;
	}

}
