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


import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configured;

import com.conversantmedia.mapreduce.tool.annotation.Option;

/**
 * A base class for annotated tool contexts that
 * includes the properties found in the concrete
 * {@link ToolContext} class.
 *
 */
public class ToolContextBase extends Configured {

	public static final String CONF_KEY_ID = ToolContextBase.class.getName() + ";ID";

	@Option
	private String input;

	@Option
	private String output;

	@Option
	private String archive;

	public String getInput() {
		return input;
	}

	public String getOutput() {
		return output;
	}

	public String getArchive() {
		return archive;
	}

	/**
	 * Retrieves the unique ID assigned
	 * to this job.
	 * 
	 * @return the generated UUID
	 */
	public String getId() {
		return getConf().get(CONF_KEY_ID);
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
