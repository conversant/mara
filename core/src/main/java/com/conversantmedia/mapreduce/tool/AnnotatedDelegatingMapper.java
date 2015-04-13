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


import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.conversantmedia.mapreduce.tool.AnnotatedDelegatingComponent;
import com.conversantmedia.mapreduce.tool.annotation.MapperService;

@MapperService
@SuppressWarnings({ "unchecked", "rawtypes" })
public final class AnnotatedDelegatingMapper<K1, V1, K2, V2> extends Mapper<K1, V1, K2, V2>
	implements AnnotatedDelegatingComponent<Mapper<K1, V1, K2, V2>> {

	public static final String CONFKEY_DELEGATE_MAPPER_CLASS = AnnotatedDelegatingMapper.class.getName() + ";delegate";

	private Mapper<K1, V1, K2, V2> mapper;

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		getDelegate(context).run(context);
		cleanup(context);
	}

	@Override
	public Mapper<K1, V1, K2, V2> getDelegate(TaskAttemptContext context) {
		if (mapper == null) {
			Class<? extends Mapper> mapperClass = (Class<? extends Mapper>) context.getConfiguration()
					.getClass(CONFKEY_DELEGATE_MAPPER_CLASS, Mapper.class);
			mapper = (Mapper<K1, V1, K2, V2>) ReflectionUtils.newInstance(
				mapperClass, context.getConfiguration());
		}
		return this.mapper;
	}
}
