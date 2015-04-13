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

import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.mapreduce.OutputFormat;

@Inherited
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AvroNamedOutput {

	/**
	 * Sets the name when you don't need any other
	 * options. Enables writing @AvroNamedOutput("myAvroOutName")
	 * Supports OGNL expressions by enclosing expression in braces, prefixed
	 * by a '$'. Example: @AvroNamedOutput("${context.property}")
	 * @return	the name for this output
	 */
	String[] value() default "default";

	/**
	 * Sets the name. Synonymous with value - designed primarily for use
	 * when you're setting multiple options and don't want to use 'value=""'
	 * Supports OGNL expressions by enclosing expression in braces, prefixed
	 * by a '$'.
	 * @return	the name for this output
	 */
	String[] name() default "default";

	/**
	 * Schema to output
	 * @return the avro record type
	 */
	Class<? extends SpecificRecordBase> record();

	/**
	 * Output format class.
	 * @return the output format class
	 */
	@SuppressWarnings("rawtypes")
	Class<? extends OutputFormat> format() default AvroKeyOutputFormat.class;

	/**
	 * MultipleOutputs.setCountersEnabled(boolean)
	 * @return boolean status of counters
	 */
	boolean countersEnabled() default false;

}
