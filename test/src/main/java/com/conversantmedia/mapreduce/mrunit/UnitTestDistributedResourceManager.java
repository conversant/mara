package com.conversantmedia.mapreduce.mrunit;

/*
 * #%L
 * Mara Unit Test Framework
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


import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.springframework.core.annotation.AnnotationUtils;

import com.conversantmedia.mapreduce.tool.DistributedResourceManager;
import com.conversantmedia.mapreduce.tool.annotation.Distribute;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationUtil;

/**
 *
 *
 */
public class UnitTestDistributedResourceManager {

	@SuppressWarnings("rawtypes") 
	public void setupDistributedResources(String testName, Class<?> clazz, Object testBean, Mapper mapper, Reducer reducer) throws Exception {
		// Search for annotated resources to distribute
		List<String> properties = new ArrayList<String>();
		addDistributedProperties(clazz, properties);

		// Now retrieve property values from unit test bean
		Map<String, Object> values = new HashMap<String, Object>();
		for (String property : properties) {
			// First look for test-specific getter suffix'd by the test name:
			String testProperty = property + "_" + testName;
			Object value = getBeanValue(testProperty, testBean);
			if (value == null) {
				value = getBeanValue(property, testBean);
			}
			if (value != null) {
				values.put(property, value);
			}
		}

		if (values.size() > 0) {
			if (mapper != null) {
				distributeValue(values, mapper);
			}
			if (reducer != null) {
				distributeValue(values, reducer);
			}
		}
	}

	protected Object getBeanValue(String property, Object bean) {
		Object value = null;
		String methodName = "get" + StringUtils.capitalize(property);
		try {
			Method method = bean.getClass().getDeclaredMethod(methodName);
			method.setAccessible(true);
			value = method.invoke(bean);
		}
		catch (NoSuchMethodException | SecurityException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			logger().info("Unable to find method [" + methodName + "] for distributed resource.");
		}

		return value;
	}

	protected void distributeValue(Map<String, Object> values, Object bean) throws IllegalArgumentException, IllegalAccessException {
		@SuppressWarnings("unchecked")
		List<Field> fields = MaraAnnotationUtil.INSTANCE.findAnnotatedFields(bean.getClass(), Resource.class);
		for (Field field : fields) {
			Resource resAnnotation = field.getAnnotation(Resource.class);
			String property = StringUtils.isEmpty(resAnnotation.name())? field.getName() : resAnnotation.name();
			Object value = values.get(property);
			if (value != null) {
				DistributedResourceManager.setFieldValue(field, bean, value);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected void addDistributedProperties(Class<?> clazz, List<String> properties) {
		Distribute distribute;
		MaraAnnotationUtil util = MaraAnnotationUtil.INSTANCE;
		List<Field> fields = util.findAnnotatedFields(clazz, Distribute.class);
		for (Field field : fields) {
			distribute = field.getAnnotation(Distribute.class);
			String prop = StringUtils.isBlank(distribute.name())? field.getName() : distribute.name();
			properties.add(prop);
		}

		List<Method> methods = util.findAnnotatedMethods(clazz, Distribute.class);
		for (Method method : methods) {
			distribute = AnnotationUtils.findAnnotation(method, Distribute.class);
			String defaultName = StringUtils.uncapitalize(StringUtils.removeStart(method.getName(), "get"));
			String prop = StringUtils.isBlank(distribute.name())? defaultName : distribute.name();
			properties.add(prop);
		}
	}

	protected Logger logger() {
		return Logger.getLogger(this.getClass());
	}
}
