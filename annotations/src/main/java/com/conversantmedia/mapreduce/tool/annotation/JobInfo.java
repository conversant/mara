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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Annotation for configuring a Map/Reduce Job.
 *
 *
 */
@Inherited
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface JobInfo {

	String value() default "";

	String name() default "";

	String numReducers() default "-1";

	MapperInfo map() default @MapperInfo;

	ReducerInfo reduce() default @ReducerInfo;

	CombinerInfo combine() default @CombinerInfo;

	Sorter sorter() default @Sorter;

	Grouping grouping() default @Grouping;

	Partitioner partitioner() default @Partitioner;

	// Used as default to mark "not set"
	public static class NULLCOMPARATOR extends WritableComparator {
		protected NULLCOMPARATOR(Class<? extends WritableComparable<?>> keyClass) {
			super(keyClass);
		}
	}

	@SuppressWarnings("rawtypes")
	public static class NULLPARTITIONER extends org.apache.hadoop.mapreduce.Partitioner {
		@Override
		public int getPartition(Object key, Object value, int numPartitions) {
			// TODO Auto-generated method stub
			return 0;
		}
	}

}
