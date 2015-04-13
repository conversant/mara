package com.conversantmedia.mapreduce.tool.event;

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


import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.conversantmedia.mapreduce.tool.AnnotatedToolContext;
import com.conversantmedia.mapreduce.tool.event.ToolEvent;

public class AnnotatedToolEvent implements ToolEvent<Object> {

	private AnnotatedToolContext context;

	public AnnotatedToolEvent(ToolEvent<AnnotatedToolContext> parent) {
		this.context = parent.getContext();
	}

	@Override
	public Object getContext() {
		return this.context.getAnnotatedBean();
	}

	@Override
	public Job getJob() {
		return this.context.getJob();
	}

	@Override
	public Configuration getConf() {
		return this.context.getJob().getConfiguration();
	}

	/**
	 * Method to get a handle on the {@link AnnotatedToolContext}.
	 * Shouldn't use unless you know what you're doing - interface
	 * subject to change!
	 * @return	the internal context wrapping the event context. 
	 */
	@Unstable
	public AnnotatedToolContext getWrappingContext() {
		return this.context;
	}

}
