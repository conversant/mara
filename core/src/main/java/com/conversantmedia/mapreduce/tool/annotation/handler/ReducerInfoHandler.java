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
import java.lang.reflect.Type;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;

/**
 *
 *
 */
@Service
public class ReducerInfoHandler extends AnnotationHandlerBase {

	@Override
	public boolean accept(Annotation annotation) {
		return annotation.annotationType() == ReducerInfo.class;
	}

	@Override @SuppressWarnings({ "rawtypes" })
	public void process(Annotation annotation, Job job, Object target) {
		ReducerInfo reduce = (ReducerInfo)annotation;
		if (reduce != null && reduce.value() != org.apache.hadoop.mapreduce.Reducer.class) {
			Class<? extends Reducer> reducerClass = reduce.value();
			job.setReducerClass(reducerClass);
			// shouldn't use defaults, but there so that we can keep the option to
			// set as a property of the JobInfo annotation instead of standalone
			configureOutputKeyValue(job, reducerClass, reduce);
		}
	}

	@SuppressWarnings("rawtypes")
	protected void configureOutputKeyValue(Job job, Class<? extends Reducer> reducerClass, ReducerInfo reducer) {
		MaraAnnotationUtil util = MaraAnnotationUtil.instance();
		// Try and work it out from the generics
		Type[] params = util.getGenericTypeParams(reducerClass, Reducer.class);

		if (reducer != null && reducer.output().key() != void.class) {
			job.setOutputKeyClass(reducer.output().key());
		}
		else if (params != null && params.length == 4){
			job.setOutputKeyClass((Class<?>)params[2]);
		}

		if (reducer != null && reducer.output().value() != void.class) {
			job.setOutputValueClass(reducer.output().value());
		}
		else if (params != null && params.length == 4){
			job.setOutputValueClass((Class<?>)params[3]);
		}
	}
}
