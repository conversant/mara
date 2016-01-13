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

import com.conversantmedia.mapreduce.tool.ToolContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class DefaultToolEvent<T extends ToolContext> implements ToolEvent<T> {

	private final T context;

	public DefaultToolEvent(T context) {
		this.context = context;
	}

	@Override
	public T getContext() {
		return context;
	}

	@Override
	public Job getJob() {
		return this.context.getJob();
	}

	@Override
	public Configuration getConf() {
		return this.context.getJob().getConfiguration();
	}
}
