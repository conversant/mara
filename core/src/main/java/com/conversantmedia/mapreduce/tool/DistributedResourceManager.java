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


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.beans.SimpleTypeConverter;
import org.springframework.core.annotation.AnnotationUtils;

import com.conversantmedia.mapreduce.tool.annotation.Distribute;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationUtil;
import com.google.common.primitives.Primitives;

/**
 * Encapsulates the logic required for distributing
 *
 */
public class DistributedResourceManager extends Configured {

	public static final String VALUE_SEP = "|";

	public static final String CONFIGKEYBASE_RESOURCE =
			DistributedResourceManager.class.getPackage().toString() + "@Resource;";

	protected DistributedResourceManager(Configuration config) {
		setConf(config);
	}

	@SuppressWarnings("unchecked")
	protected void configureBeanDistributedResources(Object annotatedBean)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException, IOException {
		MaraAnnotationUtil util = MaraAnnotationUtil.INSTANCE;
		Distribute distribute;
		// Search for annotated resources to distribute
		List<Field> fields = util.findAnnotatedFields(annotatedBean.getClass(), Distribute.class);
		for (Field field : fields) {
			distribute = field.getAnnotation(Distribute.class);
			String key = StringUtils.isBlank(distribute.name())? field.getName() : distribute.name();

			field.setAccessible(true);
			Object value = field.get(annotatedBean);
			if (value != null) {
				registerResource(key, value);
			}
		}

		// Search for annotated methods
		List<Method> methods = util.findAnnotatedMethods(annotatedBean.getClass(), Distribute.class);
		for (Method method : methods) {
			distribute = AnnotationUtils.findAnnotation(method, Distribute.class);
			// Invoke the method to retrieve the cache file path. Needs to return either a String or a Path
			String defaultName = StringUtils.uncapitalize(StringUtils.removeStart(method.getName(), "get"));
			String key = StringUtils.isBlank(distribute.name())? defaultName : distribute.name();
			Object value = method.invoke(annotatedBean);
			if (value != null) {
				registerResource(key, value);
			}
		}
	}

	/**
	 * Register this resource. If the resource is a simple property (i.e. primative or String),
	 * it will place it in the configuration. Otherwise, it uses the distributed cache
	 * mechanism as required.
	 * 
	 * @param key 			the resource key. Usually a property/field name.
	 * @param value 		the resource.
	 * @throws IOException	if resource cannot be serialized
	 *
	 */
	public void registerResource(String key, Object value) throws IOException {
		if (value == null) {
			return;
		}

		String valueString = null;

		// First, determine our approach:
		if (value instanceof String) {
			valueString = (String)value;
		}
		else if (Primitives.isWrapperType(value.getClass())) {
			valueString = String.valueOf(value);
		}
		// If this is a Path or File object we'll place it
		// on the distributed cache
		else if (value instanceof Path) {
			Path path = (Path)value;
			valueString = path.getName();

			// Distribute the file
			DistributedCache.addCacheFile(path.toUri(), getConf());
		}
		else if (value instanceof File ){
			File file = (File)value;
			valueString = file.getName();

			// Distribute the file
			distributeLocalFile(file);
		}
		// Check if it's serializable
		else if (value instanceof java.io.Serializable) {
			// Serialize the object and place it on the distributed cache
			ObjectOutputStream out = null;
			try {
				File beanSerFile = File.createTempFile(value.getClass().getName(), ".ser");
				FileOutputStream fileOut = new FileOutputStream(beanSerFile);
				out = new ObjectOutputStream(fileOut);
				out.writeObject(value);
				valueString = beanSerFile.getName();

				// Distribute the file
				distributeLocalFile(beanSerFile);
			}
			finally {
				IOUtils.closeQuietly(out);
			}
		}
		else {
			throw new IllegalArgumentException("Resource [" + key + "] is not serializable.");
		}

		// Setup the config key
		String configKey = CONFIGKEYBASE_RESOURCE + key;
		getConf().set(configKey, value.getClass().getName() + VALUE_SEP + valueString);
	}

	private Path distributeLocalFile(File file) throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		Path dest = new Path(file.getAbsolutePath());
		fs.copyFromLocalFile(true, true, new Path(file.getAbsolutePath()), dest);
		DistributedCache.addCacheFile(dest.toUri(), getConf());
		return dest;
	}

	/**
	 * Locates the resources in the configuration and distributed cache, etc., 
	 * and sets them on the provided mapper instance.
	 * 
	 * @param bean				the object to inspect for resource annotations
	 * @param config			the job configuration
	 * @throws ToolException	if there are errors with reflection or the cache
	 */
	@SuppressWarnings("unchecked")
	public static void initializeResources(Object bean, Configuration config) throws ToolException {
		try {
			List<Field> fields = MaraAnnotationUtil.INSTANCE.findAnnotatedFields(bean.getClass(), Resource.class);
			Path[] files = DistributedCache.getLocalCacheFiles(config);
			for (Field field : fields) {
				Resource resAnnotation = field.getAnnotation(Resource.class);
				String key = StringUtils.isEmpty(resAnnotation.name())? field.getName() : resAnnotation.name();
				String resourceId = config.get(CONFIGKEYBASE_RESOURCE + key);
				if (resourceId != null) {
					String[] parts = StringUtils.split(resourceId, VALUE_SEP);
					String className = parts[0];
					String valueString = parts[1];

					// Retrieve the value
					Object value = getResourceValue(bean, field, valueString, className, files, config);

					setFieldValue(field, bean, value);
				}
			}
		}
		catch (IllegalArgumentException | IOException | ClassNotFoundException | IllegalAccessException e) {
			throw new ToolException(e);
		}
	}

	public static void setFieldValue(Field field, Object bean, Object value)
			throws IllegalAccessException {
		// Set it on the field
		field.setAccessible(true);

		// Perform type conversion if possible..
		SimpleTypeConverter converter = new SimpleTypeConverter();
		try {
			value = converter.convertIfNecessary(value, field.getType());
		}
		catch (ConversionNotSupportedException e) {
			// If conversion isn't supported, ignore and try and set anyway.
		}
		field.set(bean, value);
	}

	private static Object getResourceValue(Object bean, Field field, String valueString,
			String originalTypeClassname, Path[] distFiles, Configuration config) throws IOException, ClassNotFoundException {

		// First, determine our approach:
		Object value = null;
		if (field.getType().isAssignableFrom(String.class)) {
			value = valueString;
		}
		else if (ClassUtils.isPrimitiveOrWrapper(field.getType())) {
			value = ConvertUtils.convert(valueString, field.getType());
		}
		else {
			Path path = distributedFilePath(valueString, distFiles, config);

			// This is something on the distributed cache (or illegal)
			if (field.getType() == Path.class) {
				value = path;
			}
			else if (field.getType() == File.class ){
				value = new File(path.toUri());
			}
			// Deserialize .ser file
			else if (field.getType().isAssignableFrom(Class.forName(originalTypeClassname))) {
				ObjectInputStream in = null;
				try {
					File beanSerFile = new File(path.toUri().getPath());
					FileInputStream fileIn = new FileInputStream(beanSerFile);
					in = new ObjectInputStream(fileIn);
					value = in.readObject();
				}
				finally {
					IOUtils.closeQuietly(in);
				}
			}
			else {
				throw new IllegalArgumentException("Cannot locate resource for field ["
						+ field.getName() + "]");
			}
		}

		return value;
	}

	private static Path distributedFilePath(String fileName, Path[] distFiles, Configuration config) throws IOException {
		for (Path path : distFiles) {
			if (StringUtils.equals(fileName,path.getName())) {
				return path;
			}
		}
		return null;
	}

}


