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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.conversantmedia.mapreduce.tool.annotation.handler.AnnotationHandlerProvider;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationHandler;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import com.conversantmedia.mapreduce.tool.annotation.DriverCleanup;
import com.conversantmedia.mapreduce.tool.annotation.DriverContext;
import com.conversantmedia.mapreduce.tool.annotation.DriverInit;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo;
import com.conversantmedia.mapreduce.tool.annotation.JobInit;
import com.conversantmedia.mapreduce.tool.annotation.ToolCleanup;
import com.conversantmedia.mapreduce.tool.annotation.ToolContext;
import com.conversantmedia.mapreduce.tool.annotation.ToolInit;
import com.conversantmedia.mapreduce.tool.annotation.Validate;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationUtil;

/**
 *
 *
 */
public class AnnotatedTool extends BaseTool<AnnotatedToolContext> {

	public static final String SYSPROP_ANNOTATION_HANDLER_PROVIDER = "mara.annotation.handler.provider";

	private static final String DEFAULT_ANNOTATION_HANDLER_PROVIDER = "com.conversantmedia.mapreduce.tool.annotation.handler.ReflectionAnnotationHandlerProvider";

	// The user's annotated tool bean
	private Object tool;

	// The parent context
	private AnnotatedToolContext context;

	// The field for the tool context.
	private Field contextField;

	// The field with the job.
	private Field jobField;

	// The method (if any) for initializing the annotated tool
	private List<Method> driverInitMethod;
	
	// Methods to be called to validate CLI arguments
	private List<Method> validateMethod;

	// The method (if any) for initializing the annotated tool
	// immediately following job construction.
	private List<Method> jobInitMethod;

	// The method (if any) to call before existing.
	private Method cleanupMethod;

	@SuppressWarnings("unchecked")
	public AnnotatedTool(Object tool) {

		this.tool = tool;

		// Find annotated fields and methods...
		MaraAnnotationUtil annotationUtil = MaraAnnotationUtil.INSTANCE;
		this.contextField = annotationUtil.findAnnotatedField(tool.getClass(),
				ToolContext.class, DriverContext.class);
		this.jobField = annotationUtil.findAnnotatedField(tool.getClass(), JobInfo.class);
		this.driverInitMethod = annotationUtil.findAnnotatedMethods(tool.getClass(),
				ToolInit.class, DriverInit.class);
		this.validateMethod = annotationUtil.findAnnotatedMethods(tool.getClass(),Validate.class);
		this.jobInitMethod = annotationUtil.findAnnotatedMethods(tool.getClass(), JobInit.class);
		this.cleanupMethod = annotationUtil.findAnnotatedMethod(tool.getClass(),
				ToolCleanup.class, DriverCleanup.class);
	}

	@Override
	public void init(AnnotatedToolContext context) throws ToolException, ParseException {
		List<Method> methods = new ArrayList<Method>();
		if (this.validateMethod != null) {
			methods.addAll(this.validateMethod);
		}
		if (this.driverInitMethod != null) {
			methods.addAll(this.driverInitMethod);
		}
		
		try {
			// Now call the all @Validate and @DriverInit methods
			for (Method m : methods) {
				m.invoke(this.tool);
			}

			// Initialize our annotations handlers
			initializeAnnotationHandlers();

		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof ParseException) {
				// If any of these throws a ParseException, treat pass
				// that up so we can output the CLI help message instead of 
				// a full stack trace.
				throw new ParseException(e.getCause().getMessage());
			}
			throw new ToolException(findRootException(e.getCause()));
		} catch (IllegalAccessException | IllegalArgumentException e) {
			throw new ToolException(e);
		}
	}

	private void initializeAnnotationHandlers() throws ToolException {
		try {
			String providerClassName = System.getProperty(SYSPROP_ANNOTATION_HANDLER_PROVIDER, DEFAULT_ANNOTATION_HANDLER_PROVIDER);
			AnnotationHandlerProvider provider = (AnnotationHandlerProvider) Class.forName(providerClassName).newInstance();
			MaraAnnotationUtil annotationUtil = MaraAnnotationUtil.INSTANCE;
			for (MaraAnnotationHandler handler : provider.handlers()) {
				annotationUtil.registerAnnotationHandler(handler, this);
			}
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException  e) {
			throw new ToolException(e);
		}
	}

	@Override
	public Job initJob(AnnotatedToolContext context) throws ToolException {
		Job job = null;
		try {
			job = buildJobFromAnnotation(context);
			jobField.setAccessible(true);
			jobField.set(this.tool, job);

			// Call the driver's job initialization if needed
			if (this.jobInitMethod != null) {
				for (Method initMethod : this.jobInitMethod) {
					initMethod.invoke(this.tool);
				}
			}

		} catch (Exception e) {
			throw new ToolException(findRootException(e));
		}
		return job;
	}

	@Override
	protected void jobPostInit(AnnotatedToolContext context) throws ToolException {
		Job job = context.getJob();
		DistributedResourceManager distManager = new DistributedResourceManager(job.getConfiguration());
		try {
			// Configure resources for the tool and context
			distManager.configureBeanDistributedResources(this.tool);
			distManager.configureBeanDistributedResources(context.getAnnotatedBean());
		} catch (IllegalAccessException | InvocationTargetException
				| NoSuchMethodException | IllegalArgumentException
				| IOException e) {
			throw new ToolException(findRootException(e));
		}
	}

	@Override
	protected void cleanUp(AnnotatedToolContext context) throws Exception {
		super.cleanUp(context);
		if (this.cleanupMethod != null) {
			this.cleanupMethod.invoke(this.tool);
		}
	}

	@Override
	protected AnnotatedToolContext newContext() throws ToolException {
		if (contextField != null) {
			Class<?> toolContextClass = this.contextField.getType();
			try {
				Object toolContext = ReflectionUtils.newInstance(toolContextClass, getConf());
				this.contextField.setAccessible(true);
				this.contextField.set(this.tool, toolContext);
				this.context = new AnnotatedToolContext(toolContext);
			} catch (IllegalAccessException | IllegalArgumentException e) {
				throw new ToolException("Unable to instantiate " + toolContextClass, e);
			}
		}
		else {
			this.context = new AnnotatedToolContext(new ToolContextBase());
		}

		return this.context;
	}

	public Object getToolBean() {
		return this.tool;
	}

	public AnnotatedToolContext getContext() {
		return this.context;
	}

	@Override
	public void setConf(Configuration conf) {
		if (this.tool != null && Configurable.class.isAssignableFrom(this.tool.getClass())) {
			((Configurable)this.tool).setConf(conf);
		}
		if (this.context != null) {
			// Put the ID into the context in case a job needs it.
			conf.set(ToolContextBase.CONF_KEY_ID, this.context.getId());
			if (this.context.getAnnotatedBean() != null
					&& Configurable.class.isAssignableFrom(this.context.getAnnotatedBean().getClass())) {
				((Configurable)this.context.getAnnotatedBean()).setConf(conf);
			}
		}
		super.setConf(conf);
	}

	/**
	 * Builds the job from tool class annotations.
	 * TODO is to implement a Strategy or Chain of Command pattern here to
	 * enable individual classes to handle specific annotations.
	 * @param context
	 * @return
	 * @throws Exception
	 */
	private Job buildJobFromAnnotation(AnnotatedToolContext context) throws Exception {
		Job job = new Job(getConf());
		MaraAnnotationUtil.INSTANCE.configureJobFromField(job, jobField, this.tool, context);
		return job;
	}

	/**
	 * Walks up the tree until we get a root cause that's not an InvocationTargetExcption
	 * @param exception
	 * @return
	 */
	private Throwable findRootException(Throwable exception) {
		if (exception instanceof InvocationTargetException 
				&& exception.getCause() != null) {
			return findRootException(exception.getCause());
		}
		return exception;
	}
	
}
