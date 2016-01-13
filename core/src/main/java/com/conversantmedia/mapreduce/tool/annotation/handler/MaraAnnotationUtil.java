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
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.core.annotation.AnnotationUtils;

import com.conversantmedia.mapreduce.tool.AnnotatedTool;
import com.conversantmedia.mapreduce.tool.AnnotatedToolContext;
import com.conversantmedia.mapreduce.tool.ExpressionEvaluator;
import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;

public enum MaraAnnotationUtil {

	INSTANCE;

	/**
	 * Default base packages to search if not overridden by a base-packages file.
	 */
	public static final String[] DEFAULT_SCAN_PACKAGES = new String[]{"com.conversantmedia", "net.cnvrmedia", "com.dotomi"};

	private static MaraAnnotationUtil instance;

	@Resource
	private Set<MaraAnnotationHandler> annotationHandlers;

	public void registerAnnotationHandler(MaraAnnotationHandler handler, AnnotatedTool driver) throws ToolException {
		if (this.annotationHandlers == null) {
			// retain order to ensure default handlers, added last, run last.
			this.annotationHandlers = new LinkedHashSet<>();
		}
		this.annotationHandlers.add(handler);
		handler.initialize(driver);
	}

	/**
	 *
	 * @param job							the job
	 * @param jobField						the field to retrieve annotations from
	 * @param driver						the driver bean
	 * @param context						the tool context
	 * @throws ToolException				if any issue is encountered through reflection or expression evaluation
	 */
	public void configureJobFromField(Job job, Field jobField, Object driver, AnnotatedToolContext context) throws ToolException {

		JobInfo jobInfo = jobField.getAnnotation(JobInfo.class);

		String name = StringUtils.isBlank(jobInfo.value())? jobInfo.name() : jobInfo.value();
		if (StringUtils.isBlank(name)) {
			name = defaultDriverIdForClass(driver.getClass());
		}

		name = (String)ExpressionEvaluator.instance().evaluate(driver, context, name);
		job.setJobName(name);

		if (!jobInfo.numReducers().equals("-1")) {
			if (NumberUtils.isNumber(jobInfo.numReducers())) {
				job.setNumReduceTasks(Integer.valueOf(jobInfo.numReducers()));
			}
			else {
				Object reducerValue = ExpressionEvaluator.instance().evaluate(driver, context, jobInfo.numReducers());
				if (reducerValue != null) {
					job.setNumReduceTasks((Integer)reducerValue);
				}
			}
		}

		// We can override (the runjob script does) which jar to use instead of using running driver class
		if (StringUtils.isBlank(job.getConfiguration().get("mapred.jar"))) {
			job.setJarByClass(driver.getClass());
		}

		handleJobFieldAnnotations(job, jobField, jobInfo);
	}

	/**
	 * Use any relevant annotations on the supplied class to configure
	 * the given job.
	 * 
	 * @param clazz				the class to reflect for mara-related annotations
	 * @param job				the job being configured
	 * @throws ToolException	if reflection fails for any reason
	 */
	public void configureJobFromClass(Class<?> clazz, Job job) throws ToolException {

		configureJobFromAnnotations(job, clazz.getAnnotations(), clazz);

		for (Field field : clazz.getDeclaredFields()) {
			configureJobFromAnnotations(job, field.getAnnotations(), field);
		}
		for (Method method : clazz.getDeclaredMethods()) {
			configureJobFromAnnotations(job, method.getAnnotations(), method);
		}
	}

	private void configureJobFromAnnotations(Job job, Annotation[] annotations, Object target)
			throws ToolException {
		for (MaraAnnotationHandler handler : this.annotationHandlers) {
			for (Annotation annotation : annotations) {
				if (handler.accept(annotation)) {
					handler.process(annotation, job, target);
				}
			}
		}
	}

	/**
	 * Constructs a default driver id based on the supplied class.
	 * This is the class name, minus any (Driver|Tool|Job) ending.
	 * It is also converted to lowercase and hyphenated between
	 * camelcase chars. Example: 'MyTestJob' becomes 'my-test'
	 * 
	 * @param driverClass	the driver class to use for building the id
	 * @return				the id to use for this driver
	 */
	public String defaultDriverIdForClass(Class<?> driverClass) {
		String name = driverClass.getSimpleName();
		name = name.replaceAll("(.+)(Driver|Tool|Job)", "$1");
		String[] split = StringUtils.splitByCharacterTypeCamelCase(name);
		return StringUtils.join(split, "-").toLowerCase();
	}

	protected void handleJobFieldAnnotations(Job job, Field jobField, JobInfo jobInfo)
			throws ToolException {
		// Create a list of annotations to handle.
		List<Annotation> annotations = new ArrayList<>();

		// First add in any JobInfo nested annotations that are present.
		addNestedAnnotations(annotations,
				jobInfo.map(), jobInfo.reduce(), jobInfo.combine(),
				jobInfo.sorter(), jobInfo.grouping(), jobInfo.partitioner());

		// Add in any additional annotations on the 'Job' field
		annotations.addAll(Arrays.asList(jobField.getAnnotations()));

		// Now run them all through appropriate handlers if one is registered
		for (MaraAnnotationHandler handler : this.annotationHandlers) {
			for (Annotation annotation : annotations) {
				if (handler.accept(annotation)) {
					handler.process(annotation, job, jobField);
				}
			}
		}
	}

	private void addNestedAnnotations(List<Annotation> list, Annotation...annotations) {
		for (Annotation a : annotations) {
			if (a != null) {
				list.add(a);
			}
		}
	}

	/**
	 *
	 * @param clazz				the class to reflect
	 * @param annotationClasses	the annotations to look for
	 * @return					the first field annotated with the provided list
	 */
	@SuppressWarnings("unchecked")
	public Field findAnnotatedField(Class<?> clazz, Class<? extends Annotation>...annotationClasses) {
		for (Field field : clazz.getDeclaredFields()) {
			for (Class<? extends Annotation> annotationClass : annotationClasses) {
				if (field.isAnnotationPresent(annotationClass)) {
					return field;
				}
			}
		}
		return null;
	}

	/**
	 *
	 * @param clazz				the class to reflect
	 * @param annotationClasses	the annotations to look for
	 * @return					the fields annotated with the provided list
	 */
	@SuppressWarnings("unchecked")
	public List<Field> findAnnotatedFields(Class<?> clazz, Class<? extends Annotation>...annotationClasses) {
		List<Field> fields = new ArrayList<>();
		for (Field field : clazz.getDeclaredFields()) {
			for (Class<? extends Annotation> annotationClass : annotationClasses) {
				if (field.isAnnotationPresent(annotationClass)) {
					fields.add(field);
				}
			}
		}
		return fields;
	}

	/**
	 * Convenience method for finding the first annotated method in a given class.
	 * 
	 * @param clazz				the class to reflect
	 * @param annotationClasses	the annotations to look for
	 * @return					the first method annotated with the provided list
	 */
	@SuppressWarnings("unchecked")
	public Method findAnnotatedMethod(Class<?> clazz, Class<? extends Annotation>...annotationClasses) {
		for (Method method : clazz.getDeclaredMethods()) {
			for (Class<? extends Annotation> annotationClass : annotationClasses) {
				if (AnnotationUtils.findAnnotation(method, annotationClass) != null) {
					return method;
				}
			}
		}
		return null;
	}

	/**
	 * Convenience method for finding the first annotated method in a given class.
	 * 
	 * @param clazz				the class to reflect
	 * @param annotationClasses	the annotations to look for
	 * @return					the methods annotated with the provided list
	 */
	@SuppressWarnings("unchecked")
	public List<Method> findAnnotatedMethods(Class<?> clazz, Class<? extends Annotation>...annotationClasses) {
		List<Method> methods = new ArrayList<>();
		for (Method method : clazz.getDeclaredMethods()) {
			for (Class<? extends Annotation> annotationClass : annotationClasses) {
				if (AnnotationUtils.findAnnotation(method, annotationClass) != null) {
					methods.add(method);
				}
			}
		}
		return methods;
	}

	@SuppressWarnings("unchecked")
	public static boolean hasAnyAnnotation(Class<?> clazz, Class<? extends Annotation>...annotations) {
		for (Class<? extends Annotation> annotationClass : annotations) {
			if (clazz.isAnnotationPresent(annotationClass)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * If this is a multiple output we're annotating, see if there are type parameters to
	 * use for the key/value classes.
	 * @param field	the field to inspect
	 * @return		the type parameters of the specified field.
	 */
	protected Type[] getGenericTypeParams(Field field) {
		Type genericType = field.getGenericType();
		if (genericType instanceof ParameterizedType) {
			return ((ParameterizedType)genericType).getActualTypeArguments();
		}
		return null;
	}

	/**
	 * If this is a multiple output we're annotating, see if there are type parameters to
	 * use for the key/value classes.
	 * 
	 * @param clazz				the source class to reflect				
	 * @param lastAncestorClass	specifies the bottom-most ancestor to stop at when
	 * 				walking up the tree.	
	 * @return					the parameterized types on the class.
	 */
	protected Type[] getGenericTypeParams(Class<?> clazz, Class<?> lastAncestorClass) {
		if (lastAncestorClass == null) {
			lastAncestorClass = Object.class;
		}
		while (clazz != lastAncestorClass) {
			Type[] types = getParentGenericTypeParams(clazz);
			if (types != null) {
				return types;
			}
			clazz = clazz.getSuperclass();
		}
		return null;
	}

	protected Type[] getParentGenericTypeParams(Class<?> clazz) {
		Type genericType = clazz.getGenericSuperclass();
		if (genericType instanceof ParameterizedType) {
			return toRawTypes(((ParameterizedType)genericType).getActualTypeArguments());
		}
		return null;
	}

	protected Type[] toRawTypes(Type[] types) {
		Type[] rawTypes = new Type[types.length];
		for (int i = 0; i < types.length; i ++) {
			Type t = types[i];
			if (t instanceof ParameterizedType) {
				t = ((ParameterizedType)t).getRawType();
			}
			rawTypes[i] = t;
		}
		return rawTypes;
	}

	/**
	 * Retrieves the list of packages to scan for the specified system property
	 * @param sysProp		The system property that overrides
	 * @return				the list of packages to scan
	 * @throws IOException 	if we fail to load a system resource
	 */
	public String[] getBasePackagesToScan(String sysProp, String resource) throws IOException {
		String sysPropScanPackages = System.getProperty(sysProp);
		if (StringUtils.isNotBlank(sysPropScanPackages)) {
			// The system property is set, use it.
			return StringUtils.split(sysPropScanPackages, ',');
		}
		else {
			// Search the classpath for the properties file. If not, use default
			List<String> packages = new ArrayList<>();
			InputStream resourceStream = null;
			try {
				if (resource != null) {
					resourceStream = this.getClass().getClassLoader().getResourceAsStream(resource);
				}
				if (resourceStream != null) {
					packages = IOUtils.readLines(resourceStream);
				}
				else {
					packages = Arrays.asList(DEFAULT_SCAN_PACKAGES);
				}
			}
			finally {
				IOUtils.closeQuietly(resourceStream);
			}

			return packages.toArray(new String[packages.size()]);
		}
	}
}
