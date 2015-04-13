package com.conversantmedia.mapreduce.tool.annotation;

/*
 * #%L
 * Mara Annotations/API
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


import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

@Inherited
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FileInput {

	/**
	 * Input format class.
	 * @return	The input format class
	 */
	@SuppressWarnings("rawtypes")
	Class<? extends FileInputFormat> value();

	/**
	 * The context property to use for setting the input.
	 * By default this is 'context.input' - the root context's
	 * input path.
	 * @return	the input path
	 */
	String path() default "${context.input}";

}
