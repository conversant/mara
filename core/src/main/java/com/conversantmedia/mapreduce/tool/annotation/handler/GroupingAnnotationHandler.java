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
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.annotation.Grouping;
import com.conversantmedia.mapreduce.tool.annotation.JobInfo.NULLCOMPARATOR;

/**
 *
 *
 */
@Service
public class GroupingAnnotationHandler extends AnnotationHandlerBase {

	@Override
	public boolean accept(Annotation annotation) {
		return annotation.annotationType() == Grouping.class;
	}

	@Override
	public void process(Annotation annotation, Job job, Object target) {
		Grouping grouping = (Grouping)annotation;
		if (grouping != null
				&& grouping.value() != null
				&& grouping.value() != NULLCOMPARATOR.class) {
			job.setGroupingComparatorClass(grouping.value());
		}
	}

}
