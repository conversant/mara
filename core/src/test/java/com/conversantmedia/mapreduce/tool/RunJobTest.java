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


import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.conversantmedia.mapreduce.tool.RunJob.DriverMeta;
import com.conversantmedia.mapreduce.tool.event.ToolListener;
import com.conversantmedia.mapreduce.tool.sample.AnotherTest;

/**
 * 
 *
 */
public class RunJobTest {

	RunJob runJob;
	
	@Test
	public void testScanPackages_Resource() {
		String packages = "package1,package2,package3";	
		try {
			String[] result = RunJob.getBasePackagesToScan();
			
			assertNotNull(result);
			assertThat(result.length, equalTo(3));
			assertThat(StringUtils.join(result, ","), equalTo(packages));
			
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScanPackages_SystemProperty() {
		String packages = "package1,package2,package3";
		System.setProperty(RunJob.SYSPROP_SCAN_PACKAGES, packages);
		
		try {
			String[] result = RunJob.getBasePackagesToScan();
			
			assertNotNull(result);
			assertThat(result.length, equalTo(3));
			assertThat(StringUtils.join(result, ","), equalTo(packages));
			
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test @SuppressWarnings({"unchecked"})
	public void testOutputDrivers() {

		Map<String, DriverMeta> drivers = new HashMap<String, DriverMeta>();
		// To avoid vararg null warnings...
		Class<? extends ToolListener<?>> nullListener = null;
		drivers.put("driverB", new DriverMeta("driverA",
				"short description",
				"N/A", AnotherTest.class, nullListener));
		drivers.put("driverC", new DriverMeta("driverA",
				"a very very long description for this driver that we need to wrap.",
				"123.0.3839-SNAPSHOT", AnotherTest.class, nullListener));
		drivers.put("driverA", new DriverMeta("driverA",
				"",
				"N/A", AnotherTest.class, nullListener));

		RunJob.outputDriversTable(drivers);
	}
	
	@Before
	public void setup() {
		runJob = new RunJob();
		
		// Ensure our system property is unset before each run
		System.clearProperty(RunJob.SYSPROP_SCAN_PACKAGES);
	}
	
	@After 
	public void tearDown() {
		runJob = null;
	}
}
