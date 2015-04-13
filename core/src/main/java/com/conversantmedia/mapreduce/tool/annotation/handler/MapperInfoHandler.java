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
import java.lang.reflect.Type;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.annotation.MapperInfo;
import com.conversantmedia.mapreduce.tool.annotation.ReducerInfo;

@Service
public class MapperInfoHandler extends AnnotationHandlerBase {

	@Override
	public boolean accept(Annotation annotation) {
		return annotation.annotationType() == MapperInfo.class;
	}

	@Override @SuppressWarnings("rawtypes")
	public void process(Annotation annotation, Job job, Object target) {
		MapperInfo map = (MapperInfo)annotation;
		if (map.value() != org.apache.hadoop.mapreduce.Mapper.class) {
			Class<? extends Mapper> mapperClass = map.value();
			job.setMapperClass(mapperClass);
			
			// Is this a map-only job?
			Field jobField = (Field)target;
			boolean isMapOnly = isMapOnlyJob(job, jobField);
			
			configureOutputKeyValue(job, mapperClass, map, isMapOnly);
		}
	}

	/**
	 * Is this a map-only job?
	 * 
	 * @param job		the job
	 * @param jobField	the field to reflect for annotations
	 * @return			<code>true</code> if map only, <code>false</code> otherwise.
	 */	
	protected boolean isMapOnlyJob(Job job, Field jobField) {
		if (job.getNumReduceTasks() > 0) {
			return false;
		}
		
		// See if we have a ReducerInfo annotation - otherwise 
		// we'll consider this a "map only" job
		return !jobField.isAnnotationPresent(ReducerInfo.class);
	}

	@SuppressWarnings("rawtypes")
	public void configureOutputKeyValue(Job job, Class<? extends Mapper> mapperClass, 
			MapperInfo map, boolean isMapOnly) {
		MaraAnnotationUtil util = MaraAnnotationUtil.instance();
		// Try and work it out from the generics
		Type[] params = util.getGenericTypeParams(mapperClass, Mapper.class);

		int length = 4;
		int keyIdx = 2;
		int valueIdx = 3;

		// Special case for TableMapper - assume if there are only two, they are the output key/value
		// TODO resolve this hack - force explicit?
		if (params != null && params.length == 2) {
			length = 2;
			keyIdx = 0;
			valueIdx = 1;
		}

		if (map != null && map.output().key() != void.class) {
			job.setMapOutputKeyClass(map.output().key());
			if (isMapOnly) {
				job.setOutputKeyClass(map.output().key());
			}
		}
		else if (params != null && params.length == length){
			job.setMapOutputKeyClass((Class<?>)params[keyIdx]);
			if (isMapOnly) {
				job.setOutputKeyClass((Class<?>)params[keyIdx]);
			}
		}

		if (map != null && map.output().value() != void.class) {
			job.setMapOutputValueClass(map.output().value());
			if (isMapOnly) {
				job.setOutputValueClass(map.output().value());
			}
		}
		else if (params != null && params.length == length){
			job.setMapOutputValueClass((Class<?>)params[valueIdx]);
			if (isMapOnly) {
				job.setOutputValueClass((Class<?>)params[valueIdx]);
			}
		}
	}

}
