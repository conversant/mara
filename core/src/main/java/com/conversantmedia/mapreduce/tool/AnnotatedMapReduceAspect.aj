package com.conversantmedia.mapreduce.tool;

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
import java.lang.reflect.Field;
import java.util.List;

import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.AnnotatedDelegatingComponent;
import com.conversantmedia.mapreduce.tool.IMapperService;
import com.conversantmedia.mapreduce.tool.IReducerService;
import com.conversantmedia.mapreduce.tool.annotation.AvroNamedOutput;
import com.conversantmedia.mapreduce.tool.annotation.MapperService;
import com.conversantmedia.mapreduce.tool.annotation.NamedOutput;
import com.conversantmedia.mapreduce.tool.annotation.ReducerService;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationUtil;

/**
 * Aspect for handling annotated Mappers/Reducers.
 * @author Patrick Jaromin <pjaromin@dotomi.com>
 *
 */
public aspect AnnotatedMapReduceAspect {
	
	/**
	 * Not presently utilized. This is reserved for future possible use.
	 */
	declare parents : (@MapperService *) implements IMapperService;
	declare parents : (@ReducerService *) implements IReducerService;

	/**
	 * Pointcut that intercepts calls to job.setXXXClass so we can dynamically replace with the annotated
	 * delegate for mappers, reducers, combiners for resource injection
	 */
	pointcut setComponentClass(Class clazz, Job job):
		(call(void org.apache.hadoop.mapreduce.Job.setMapperClass(Class))
		|| call(void org.apache.hadoop.mapreduce.Job.setReducerClass(Class))
		|| call(void org.apache.hadoop.mapreduce.Job.setCombinerClass(Class))) && target(job) && args(clazz);
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void around(Class clazz, Job job): setComponentClass(clazz, job) {
		boolean isMapperService = MaraAnnotationUtil.hasAnyAnnotation(clazz, MapperService.class, Service.class);
		boolean isReducerService = MaraAnnotationUtil.hasAnyAnnotation(clazz, ReducerService.class, Service.class);
		
		// First, check for annotations requiring job configuration
		try {
			if (isMapperService || isReducerService) {
				MaraAnnotationUtil.INSTANCE.configureJobFromClass(clazz, job);
			}
		} catch (ToolException e) {
			// For now, just print out and continue
			e.printStackTrace();
		}

		// Now, replace with the annotated component if needed
		if (Mapper.class.isAssignableFrom(clazz)) {
			if (isMapperService) {				
				// Need to use delegate and set the real mapper in the configuration
				job.getConfiguration().set(AnnotatedDelegatingMapper.CONFKEY_DELEGATE_MAPPER_CLASS, clazz.getName());
				clazz = AnnotatedDelegatingMapper.class;
			}
		}
		else if (Reducer.class.isAssignableFrom(clazz)) {
			String methodName = thisJoinPoint.getSignature().getName();
			if (isReducerService) {
				if (StringUtils.contains(methodName, "Reducer")) {
					// Need to use delegate and set the real mapper in the configuration
					job.getConfiguration().set(AnnotatedDelegatingReducer.CONFKEY_DELEGATE_REDUCER_CLASS, clazz.getName());
					clazz = AnnotatedDelegatingReducer.class;					
				}
				else {
					// Must be a combiner
					job.getConfiguration().set(AnnotatedDelegatingCombiner.CONFKEY_DELEGATE_COMBINER_CLASS, clazz.getName());
					clazz = AnnotatedDelegatingCombiner.class;
				}

			}
		}

		proceed(clazz, job);
	}

	/**
	 * Pointcut for intercepting the run of the delegated mapper so we can perform resource injection.
	 * @param context
	 */
	pointcut componentRun(TaskInputOutputContext context):
		(execution (void com.conversantmedia.mapreduce.tool.AnnotatedDelegating*.run(org.apache.hadoop.mapreduce.*.Context))) && args(context);

	@SuppressWarnings("rawtypes")
	before(TaskInputOutputContext context): componentRun(context) {
		AnnotatedDelegatingComponent component = (AnnotatedDelegatingComponent)thisJoinPoint.getThis();
		try {
			DistributedResourceManager.initializeResources(component.getDelegate(context), context.getConfiguration());
			intializeMultipleOutputs(component.getDelegate(context), context);
		}
		catch (Exception e) {
			e.printStackTrace();
		}	
	}

	@SuppressWarnings("rawtypes")
	after(TaskInputOutputContext context): componentRun(context) {
		AnnotatedDelegatingComponent mapper = (AnnotatedDelegatingComponent)thisJoinPoint.getThis();
		try {
			closeMultipleOutputs(mapper.getDelegate(context), context);
		}
		catch (Exception e) {
			e.printStackTrace();
		}	
	}

	@SuppressWarnings({ "rawtypes", "unchecked" }) 
	private void intializeMultipleOutputs(Object bean, TaskInputOutputContext context) throws IllegalArgumentException, IllegalAccessException {
		List<Field> fields = MaraAnnotationUtil.INSTANCE.findAnnotatedFields(bean.getClass(), NamedOutput.class, AvroNamedOutput.class);
		for (Field field : fields) {
			NamedOutput namedOutAnnotation = field.getAnnotation(NamedOutput.class);
			if (namedOutAnnotation != null) {
				// Construct a new multiple output and set on the bean
				MultipleOutputs multipleOutputs = new MultipleOutputs(context);
				field.setAccessible(true);
				field.set(bean, multipleOutputs);
			}
			
			AvroNamedOutput avroNamedOutputAnnotation = field.getAnnotation(AvroNamedOutput.class);
			if (avroNamedOutputAnnotation != null) {
				AvroMultipleOutputs avroMultipleOutputs = new AvroMultipleOutputs(context);
				field.setAccessible(true);
				field.set(bean, avroMultipleOutputs);
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" }) 
	private void closeMultipleOutputs(Object bean, TaskInputOutputContext context) 
			throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
		List<Field> fields = MaraAnnotationUtil.INSTANCE.findAnnotatedFields(bean.getClass(), NamedOutput.class, AvroNamedOutput.class);
		for (Field field : fields) {
			NamedOutput namedOutAnnotation = field.getAnnotation(NamedOutput.class);
			if (namedOutAnnotation != null) {
				// Close any we've created
				field.setAccessible(true);
				MultipleOutputs multiOut = (MultipleOutputs)field.get(bean);
				multiOut.close();
			}
			
			AvroNamedOutput avroNamedOutputAnnotation = field.getAnnotation(AvroNamedOutput.class);
			if (avroNamedOutputAnnotation != null) {
				field.setAccessible(true);
				AvroMultipleOutputs avroMultiOut = (AvroMultipleOutputs)field.get(bean);
				avroMultiOut.close();
			}
		}
	}
}
