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

/**
 * Annotation for specifying input from a single table.
 *
 */
@Inherited
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface TableInput {

	String table() default "${context.input}";

	String scanProperty() default "scan";

	// Alternative for setting mapper here instead of separate.
	// May be needed in future if we allow separate mappers 
	// for each table input in a MultiTableInput.
	MapperInfo mapper() default @MapperInfo;
	
	// RESERVED for future use
	Scanner scan() default @Scanner;
}
