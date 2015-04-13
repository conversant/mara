package com.conversantmedia.mapreduce;

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


/**
 * 
 */
public class MapReduceConstants {
	
	public static final String CONF_KEY_NAMESPACE = "cnvr.mapreduce";
	public static final String CONF_KEY_PREFIX = CONF_KEY_NAMESPACE + ".";

	// Driver metadata
	public static final String CONF_KEY_DRIVER_ID = CONF_KEY_PREFIX + "driver.id";
	public static final String CONF_KEY_DRIVER_DESCRIPTION = CONF_KEY_PREFIX + "driver.description";
	public static final String CONF_KEY_DRIVER_VERSION = CONF_KEY_PREFIX + "driver.version";
	public static final String CONF_KEY_DRIVER_CLASS = CONF_KEY_PREFIX + "driver.class";

	// Our job context uuid
	public static final String CONF_KEY_JOB_UUID = CONF_KEY_PREFIX + "job.uuid";

}
