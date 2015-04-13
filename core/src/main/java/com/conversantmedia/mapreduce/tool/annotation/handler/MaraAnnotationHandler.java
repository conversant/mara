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

import org.apache.hadoop.mapreduce.Job;

import com.conversantmedia.mapreduce.tool.AnnotatedTool;
import com.conversantmedia.mapreduce.tool.ToolException;

/**
 * Implement this interface when you introduce a new 
 * annotation. 
 * Consider extending {@link AnnotationHandlerBase} and  
 * be certain to mark your new class as a {@link org.springframework.stereotype.Service}
 * to ensure it's discovered by the container.
 *
 */
public interface MaraAnnotationHandler {

	/**
	 * Can this handler handle this annotation?
	 * 
	 * @param annotation 		the annotation under consideration
	 * @return					<code>true</code> if acceptable, <code>false</code> otherwise
	 * @throws ToolException	if the process encounters an exception of any kind
	 */
	boolean accept(Annotation annotation) throws ToolException;

	/**
	 * Process the annotation.
	 * 
	 * @param annotation		the annotation to apply to the configuration
	 * @param job				the job under configuration
	 * @param target 			object annotation is applied to (class, field, or method)
	 * @throws ToolException	if the process encounters an exception of any kind
	 */
	void process(Annotation annotation, Job job, Object target) throws ToolException;

	void initialize(AnnotatedTool tool) throws ToolException;
}
