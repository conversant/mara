package com.conversantmedia.mapreduce.tool.annotation;

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


import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.AnnotatedTool;
import com.conversantmedia.mapreduce.tool.AnnotatedToolContext;
import com.conversantmedia.mapreduce.tool.ToolException;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationHandler;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationUtil;

//@RunWith(PowerMockRunner.class)
@PrepareForTest(MultipleInputs.class)
public class JobAnnotationUtilTest {

	MaraAnnotationUtil annotationUtil;
	Job job;

	@Test @Ignore // NOT WORKING
	public void testConfigureJobFromClass() {
		Class<?> clazz = TestMapper.class;

		try {
			PowerMockito.mockStatic(MultipleInputs.class);

			// Now configure
			PowerMockito.verifyStatic(Mockito.times(6));
			annotationUtil.configureJobFromClass(clazz, job);

		} catch (ToolException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDefaultDriverName() {

		Class<?>[] drivers = { ADummy.class, ADummyTool.class,
						ADummyDriver.class, ADummyJob.class };
		for (Class<?> driverClass : drivers) {
			String name = annotationUtil.defaultDriverIdForClass(driverClass);
			assertThat(name, equalTo("a-dummy"));
		}
	}

	public static final class ADummy {}
	public static final class ADummyTool {}
	public static final class ADummyDriver {}
	public static final class ADummyJob {}

	@Before
	public void setup() throws ToolException {
		annotationUtil = MaraAnnotationUtil.instance();
		// Initialize our annotations handlers

		Configuration conf = new Configuration();
		this.job = mock(Job.class);
		when(job.getConfiguration()).thenReturn(conf);

		AnnotatedTool tool = mock(AnnotatedTool.class);
		AnnotatedToolContext context = mock(AnnotatedToolContext.class);
		when(tool.getContext()).thenReturn(context);

		try {
			Reflections reflections = new Reflections("com.conversantmedia.mapreduce.tool.annotation");
			Set<Class<?>> handlerClasses = reflections.getTypesAnnotatedWith(Service.class);
			for (Class<?> handlerClass : handlerClasses) {
				if (MaraAnnotationHandler.class.isAssignableFrom(handlerClass)) {
					MaraAnnotationHandler handler = (MaraAnnotationHandler) handlerClass.newInstance();
					annotationUtil.registerAnnotationHandler(handler, tool);
				}
			}
		} catch (InstantiationException | IllegalAccessException e) {
			throw new ToolException(e);
		}
	}

}
