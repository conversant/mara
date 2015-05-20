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


import static com.conversantmedia.mapreduce.MapReduceConstants.CONF_KEY_DRIVER_CLASS;
import static com.conversantmedia.mapreduce.MapReduceConstants.CONF_KEY_DRIVER_DESCRIPTION;
import static com.conversantmedia.mapreduce.MapReduceConstants.CONF_KEY_DRIVER_ID;
import static com.conversantmedia.mapreduce.MapReduceConstants.CONF_KEY_DRIVER_VERSION;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.jar.Manifest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.reflections.Reflections;
import org.reflections.ReflectionsException;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Service;

import com.conversantmedia.mapreduce.tool.annotation.Driver;
import com.conversantmedia.mapreduce.tool.annotation.Hidden;
import com.conversantmedia.mapreduce.tool.annotation.Tool;
import com.conversantmedia.mapreduce.tool.annotation.handler.DefaultInputAnnotationHandler;
import com.conversantmedia.mapreduce.tool.annotation.handler.DefaultOutputAnnotationHandler;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationUtil;
import com.conversantmedia.mapreduce.tool.annotation.handler.MaraAnnotationHandler;
import com.conversantmedia.mapreduce.tool.event.AnnotatedToolListener;
import com.conversantmedia.mapreduce.tool.event.ToolListener;

/**
 * Runs annotated tools.
 *
 */
public class RunJob {

	/**
	 * Default base packages to search if not overridden by a base-packages file.
	 */
	public static final String[] DEFAULT_SCAN_PACKAGES = new String[]{"com.conversantmedia", "net.cnvrmedia", "com.dotomi"};

	/**
	 * System property for overriding base packages to scan for 
	 * mara components - i.e. annotation handlers, etc.
	 */
	public static final String SYSPROP_MARA_SCAN_PACKAGES = "mara.components.packages";

	/**
	 * System property for overriding base packages to scan for
	 * {@literal @} Driver annotations.
	 */
	public static final String SYSPROP_DRIVER_SCAN_PACKAGES = "mara.drivers.packages";

	/**
	 * Packages to scan for @Drivers
	 */
	public static final String RESOURCE_DRIVER_SCAN_PACKAGES = "META-INF/mara/package-scan";

	public static void main(String[] args) throws ToolException, IOException {

		// Get the base packages from the classpath resource
		String[] scanPackages = getBasePackagesToScanForDrivers();

		// Initialize the reflections object
		Reflections reflections = initReflections((Object[])scanPackages);

		// Search the classpath for Tool and Driver annotations
		Map<String, DriverMeta> idMap = findAllDrivers(reflections);

		if (idMap.isEmpty()) {
			System.out.printf("No drivers found in package(s) [%s]\n",StringUtils.join(scanPackages, ","));
			System.exit(0);
		}

		// Expects the first argument to be the id of the
		// tool to run. Otherwise list them all:
		if (args.length < 1) {
			outputDriversTable(idMap);
			System.exit(0);
		}

		// Shift off the first (driver id) argument
		String id = args[0];
		args = ArrayUtils.subarray(args, 1, args.length);

		DriverMeta driverMeta = idMap.get(id);
		if (driverMeta == null) {
			if (StringUtils.isNotBlank(id) && !StringUtils.startsWith(id, "-")) { // don't output message if no driver was specified
													// or if the first arg is an argument such as --conf (from runjob script)
				System.out.println("No Tool or Driver class found with id [" + id + "]");
			}
			outputDriversTable(idMap);
			System.exit(1);
		}

		// Finally, run the tool
		runDriver(driverMeta, args);
	}

	/**
	 * Runs the tool given the supplied arguments.
	 * 
	 * @param args			raw command line arguments
	 * @param driverMeta	description of the driver to execute
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected static void runDriver(DriverMeta driverMeta, String[] args) {
		try {

			Configuration config = new Configuration();
			driverMeta.addToConfig(config);

			Class<?> driverClass = driverMeta.driverClass;
			BaseTool driver;
			if (BaseTool.class.isAssignableFrom(driverClass)) {
				driver = (BaseTool) driverClass.newInstance();
			} else {
				driver = new AnnotatedTool(driverClass.newInstance());
				initializeDriverResources((AnnotatedTool)driver);
				if (driverMeta.listener[0] != Tool.NULLLISTENER.class) {
					for (Class<? extends ToolListener> listenerClass : driverMeta.listener) {
						ToolListener listener = listenerClass.newInstance();
						if (Configurable.class.isAssignableFrom(listenerClass)) {
							((Configurable)listener).setConf(config);
						}
						driver.addListener(new AnnotatedToolListener(listener));
					}
				}
			}

			// Call Hadoops' 'ToolRunner' to kick off this tool
			int res = ToolRunner.run(config, driver, args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Performs resource injection in absence of Spring (which we're not prepared
	 * to add just yet.)
	 * 
	 * @param driver			the driver instance
	 * @throws ToolException	if the driver resources cannot be initialized
	 */
	private static void initializeDriverResources(AnnotatedTool driver) throws ToolException {
		// Initialize our annotations handlers
		try {
			MaraAnnotationUtil annotationUtil = MaraAnnotationUtil.instance();
			Reflections reflections = initReflections(getBasePackagesToScanForComponents());
			Set<Class<?>> handlerClasses = reflections.getTypesAnnotatedWith(Service.class);
			for (Class<?> handlerClass : handlerClasses) {
				if (MaraAnnotationHandler.class.isAssignableFrom(handlerClass)) {
					MaraAnnotationHandler handler = (MaraAnnotationHandler) handlerClass.newInstance();
					annotationUtil.registerAnnotationHandler(handler, driver);
				}
			}
			
			// Register our default annotation handlers last. The order of execution 
			// is determined by the order in which they're added.
			// TODO Should we develop a method of explicit ordering, priority so the
			// default handlers always trigger last through normal means (@Service annotation)
			annotationUtil.registerAnnotationHandler(new DefaultInputAnnotationHandler(), driver);
			annotationUtil.registerAnnotationHandler(new DefaultOutputAnnotationHandler(), driver);

		} catch (InstantiationException | IllegalAccessException | IOException e) {
			throw new ToolException(e);
		}
	}

	protected static String[] getBasePackagesToScanForDrivers() throws IOException {
		return getBasePackagesToScan(SYSPROP_DRIVER_SCAN_PACKAGES, RESOURCE_DRIVER_SCAN_PACKAGES);
	}

	protected static String[] getBasePackagesToScanForComponents() throws IOException {
		return getBasePackagesToScan(SYSPROP_MARA_SCAN_PACKAGES, null);
	}
	/**
	 * Retrieves the list of packages to scan for the specified system property
	 * @param sysProp		The system property that overrides
	 * @return				the list of packages to scan
	 * @throws IOException 	if we fail to load a system resource
	 */
	protected static String[] getBasePackagesToScan(String sysProp, String resource) throws IOException {
		String sysPropScanPackages = System.getProperty(sysProp);
		if (StringUtils.isNotBlank(sysPropScanPackages)) {
			// The system property is set, use it.
			return StringUtils.split(sysPropScanPackages, ',');
		}
		else {
			// Search the classpath for the properties file. If not, use default
			List<String> packages = new ArrayList<String>();
			InputStream resourceStream = null;
			try {
				if (resource != null) {
					resourceStream = RunJob.class.getClassLoader().getResourceAsStream(resource);
				}
				if (resourceStream != null) {
					packages = IOUtils.readLines(resourceStream);
				}
				else {
					packages = Arrays.asList(DEFAULT_SCAN_PACKAGES);
				}
			}
			finally {
				IOUtils.closeQuietly(resourceStream);
			}
			
			return packages.toArray(new String[]{});
		}
	}

	protected static Reflections initReflections(Object...packages) {
		return new Reflections(packages);
	}

	@SuppressWarnings("unchecked")
	protected static Map<String, DriverMeta> findAllDrivers(Reflections reflections) {
		Map<String, DriverMeta> idMap = new TreeMap<String, DriverMeta>( new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				return s1.compareTo(s2);
			}
		});

		for (Class<?> c : reflections.getTypesAnnotatedWith(Driver.class)) {
			Driver d = AnnotationUtils.findAnnotation(c, Driver.class);
			String version = versionForDriverClass(c, d.version());
			String driverId = d.value();
			if (StringUtils.isBlank(driverId)) {
				driverId = MaraAnnotationUtil.instance().defaultDriverIdForClass(c);
			}
			DriverMeta meta = new DriverMeta(driverId, d.description(), version, c, d.listener());
			idMap.put(driverId, meta);

			if (c.isAnnotationPresent(Hidden.class)) {
				meta.hidden = true;
			}
		}
		// Have to do this 2x - a second time for Tool
		for (Class<?> c : reflections.getTypesAnnotatedWith(Tool.class)) {
			Tool t = AnnotationUtils.findAnnotation(c, Tool.class);
			String version = versionForDriverClass(c, t.version());
			String toolId = t.value();
			if (StringUtils.isBlank(toolId)) {
				toolId = MaraAnnotationUtil.instance().defaultDriverIdForClass(c);
			}
			DriverMeta meta = new DriverMeta(toolId, t.description(), version, c, t.listener());
			idMap.put(toolId, meta);

			if (c.isAnnotationPresent(Hidden.class)) {
				meta.hidden = true;
			}
		}
		return idMap;
	}

	private static String versionForDriverClass(Class<?> c, String annotatedVersion) {
		String version = annotatedVersion;
		String manifestVersion = c.getPackage().getImplementationVersion();
		if (StringUtils.isBlank(manifestVersion)) {
			manifestVersion = readVersionFromManifest(version);
		}
		if (version.equals("N/A") && StringUtils.isNotBlank(manifestVersion)) {
			version = manifestVersion;
		}
		return version;
	}

	private static String readVersionFromManifest(String version) {
		// Try again to see if this class's MANIFEST was unjar'd by the hadoop command
		// - read it from the unjar'd file instead.
		InputStream in = null;
		try {
			URL manifestUrl = findManifestForDriver();
			if (manifestUrl != null) {
				in = manifestUrl.openStream();
				Manifest manifest = new Manifest(in);
				if (manifest != null) {
					version = manifest.getMainAttributes().getValue("Implementation-Version");
				}
			}
		} catch (Exception e) {
			// No point in exiting the app because we fail to read version from manifest.
			e.printStackTrace();
		}
		finally {
			IOUtils.closeQuietly(in);
		}
		return version;
	}

	/**
	 * Finds the first non-jar'd MANIFEST.MF. We assume this one is it.
	 * No good way to determine otherwise, but there should only be one that
	 * isn't jar'd if run via hadoop command.
	 * 
	 * @return	location of the manifest 
	 */
	private static URL findManifestForDriver() {
		try {
			Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources("META-INF/MANIFEST.MF");
			while (resources.hasMoreElements()) {
				URL manifestUrl = resources.nextElement();
				// If it is a 'jar' we would have gotten from getImplementationVersion call
				if (!StringUtils.equals("jar", manifestUrl.getProtocol())) {
					return manifestUrl;
				}
			}
		} catch (IOException ex) {
			throw new IllegalStateException(ex);
		}
		return null;
	}

	/**
	 * Outputs the driver's list
	 * 
	 * @param driversMap	map of driver metadata to output to console
	 */
	protected static void outputDriversTable(Map<String, DriverMeta> driversMap) {

		String[] colNames = new String[] {"Name","Description","Ver","Class"};
		String[] aligns = new String[] {"-","-","-","-"};
		int maxDescriptionWidth = 48;
		int widths[] = new int[colNames.length];
		for (int i = 0; i < colNames.length; i ++) {
			widths[i] = colNames[i].length();
		}
		int padding = 2;
		for (Entry<String, DriverMeta> e : driversMap.entrySet()) {
			if (!e.getValue().hidden) {
				int i = 0;
				widths[i] = Math.max(e.getKey().length(), widths[i]);
				widths[i+1] = Math.min(Math.max(e.getValue().description.length(), widths[i+1]), maxDescriptionWidth);
				widths[i+2] = Math.max(e.getValue().version.length(), widths[i+2]);
				widths[i+3] = Math.max(e.getValue().driverClass.getName().length(), widths[i+3]);
			}
		}

		// sum widths
		int width = padding * widths.length-1;
		for (int w : widths) { width+= w; }

		String sep = StringUtils.repeat("=", width);
		System.out.println(sep);
		System.out.println(StringUtils.center("A V A I L A B L E    D R I V E R S", width));
		System.out.println(sep);
		String[] underscores = new String[colNames.length];
		StringBuffer headersFormatSb = new StringBuffer();
		StringBuffer valuesFormatSb = new StringBuffer();
		for (int i = 0; i < widths.length; i++) {
			headersFormatSb.append("%-" + (widths[i]+padding) + "s");
			valuesFormatSb.append("%" + aligns[i] + (widths[i]+padding) + "s");
			underscores[i] = StringUtils.repeat("-", widths[i]);
		}
		String format = headersFormatSb.toString();
		System.out.format(format, (Object[])colNames);
		System.out.println();
		System.out.format(format, (Object[])underscores);
		System.out.println();

		format = valuesFormatSb.toString();
		List<String> descriptionLines = new ArrayList<String>();
		for (Entry<String, DriverMeta> e : driversMap.entrySet()) {
			if (!e.getValue().hidden) {
				descriptionLines.clear();
				String description = e.getValue().description;
				if (description.length() > maxDescriptionWidth) {
					splitLine(descriptionLines, description, maxDescriptionWidth);
					description = descriptionLines.remove(0);
				}
				System.out.format(format,  e.getKey(), description, StringUtils.center(e.getValue().version, widths[2]), e.getValue().driverClass.getName());
				System.out.println();
				while(!descriptionLines.isEmpty()) {
					System.out.format(format, "", descriptionLines.remove(0), "", "");
					System.out.println();
				}
			}
		}
	}

	private static void splitLine(List<String> lines, String text, int maxLength) {
		BreakIterator boundary = BreakIterator.getLineInstance();
		boundary.setText(text);
		int start = boundary.first();
		int end = boundary.next();
		int lineLength = 0;
		StringBuffer buffer = new StringBuffer();
		while (end != BreakIterator.DONE) {
			String word = text.substring(start, end);
			lineLength = lineLength + word.length();
			if (lineLength > maxLength) {
				lineLength = word.length();
				lines.add(buffer.toString());
				buffer.setLength(0);
			}
			buffer.append(word);
			start = end;
			end = boundary.next();
		}
		lines.add(buffer.toString());
	}

	/**
	 * Driver/Tool meta information
	 *
	 */
	@SuppressWarnings("rawtypes")
	protected static class DriverMeta {
		public DriverMeta(String id, String description, String version, Class<?> clazz, @SuppressWarnings("unchecked") Class<? extends ToolListener>...listenerClass) {
			this.id = id;
			this.description = description;
			this.driverClass = clazz;
			this.version = version;
			this.listener = listenerClass;
		}

		public String id;
		public String description;
		public String version;
		public Class<?> driverClass;
		public Class<? extends ToolListener>[] listener;
		public boolean hidden = false;

		public void addToConfig(Configuration conf) {
			conf.set(CONF_KEY_DRIVER_ID, id);
			conf.set(CONF_KEY_DRIVER_DESCRIPTION, description);
			conf.set(CONF_KEY_DRIVER_VERSION, version);
			conf.set(CONF_KEY_DRIVER_CLASS, driverClass.getName());
		}

	}
}
