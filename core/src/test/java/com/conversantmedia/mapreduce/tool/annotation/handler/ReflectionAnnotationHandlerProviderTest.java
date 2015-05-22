package com.conversantmedia.mapreduce.tool.annotation.handler;

import com.conversantmedia.mapreduce.tool.ToolException;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import test.annotation.handlers.Last1TestAnnotationHandler;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by pjaromin on 5/21/2015.
 */
public class ReflectionAnnotationHandlerProviderTest {

    ReflectionAnnotationHandlerProvider provider;

    static final int BASE_HANDLER_COUNT = 17;
    static final int DUMMY_HANDLER_COUNT = 4;

    @Test
    public void testSkipSomeHandlers() {
        System.setProperty("mara.annotation.handler.scan", "test.annotation.handlers");
        System.setProperty("mara.skip.annotation.handlers",
                StringUtils.join(",", new String[]{
                        test.annotation.handlers.Dummy1TestAnnotationHandler.class.getName(),
                        test.annotation.handlers.Dummy2TestAnnotationHandler.class.getName(),
                        test.annotation.handlers.Dummy3TestAnnotationHandler.class.getName()
                }));
        try {
            List<MaraAnnotationHandler> results = getMaraAnnotationHandlers();

            // Did we find them all?
            assertThat(results.size(), equalTo(1));
            assertThat(results.get(0).getClass().getName(), equalTo(Last1TestAnnotationHandler.class.getName()));

        } catch (ToolException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFindsDefaultHandlersInProperOrder() {

        try {
            List<MaraAnnotationHandler> results = getMaraAnnotationHandlers();

            // Did we find them all?
            assertThat(results.size(), equalTo(BASE_HANDLER_COUNT));

            // Last two should be the default handlers...ensure they are.
            for (int i = 0; i < results.size() - 2; i++) {
                assertFalse(results.get(i).runLast());
            }
            assertTrue(results.get(results.size()-2).runLast());
            assertTrue(results.get(results.size()-1).runLast());

        } catch (ToolException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

    }

    @Test
    public void testFindHandlersInOverriddenScanPackage() {
        System.setProperty("mara.annotation.handler.scan", "test.annotation.handlers");

        try {
            List<MaraAnnotationHandler> results = getMaraAnnotationHandlers();

            // Did we find them all?
            assertThat(results.size(), equalTo(DUMMY_HANDLER_COUNT));

            // Last two should be the default handlers...ensure they are.
            for (int i = 0; i < results.size() - 1; i++) {
                MaraAnnotationHandler handler = results.get(i);
                assertFalse(handler.runLast());
                assertThat(handler.getClass().getSimpleName(), startsWith("Dummy")); // don't care what order
            }
            // Last one should be the run last...
            MaraAnnotationHandler handler = results.get(results.size() - 1);
            assertThat(handler.getClass().getSimpleName(), startsWith("Last"));
            assertTrue(handler.runLast());

        } catch (ToolException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFindHandlersInMultipleScanPackages() {
        System.setProperty("mara.annotation.handler.scan", "test.annotation.handlers,com.conversantmedia.mapreduce.tool.annotation.handler");

        try {
            List<MaraAnnotationHandler> results = getMaraAnnotationHandlers();

            // Did we find them all?
            assertThat(results.size(), equalTo(DUMMY_HANDLER_COUNT + BASE_HANDLER_COUNT));

            // Last two should be the default handlers...ensure they are.
            for (int i = 0; i < results.size() - 3; i++) {
                MaraAnnotationHandler handler = results.get(i);
                assertFalse(handler.runLast());
            }
            // Last one should be the run last...
            for (int i = results.size(); i > results.size() - 4; i--) {
                MaraAnnotationHandler handler = results.get(results.size() - 1);
                assertTrue(handler.runLast());
            }

        } catch (ToolException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private List<MaraAnnotationHandler> getMaraAnnotationHandlers() throws ToolException {
        List<MaraAnnotationHandler> results = new ArrayList<MaraAnnotationHandler>();
        for (MaraAnnotationHandler handler : provider.handlers()) {
            results.add(handler);
//                System.out.println(handler.getClass().getName());
        }
        return results;
    }

    @Before
    public void setup() {
        System.clearProperty("mara.annotation.handler.scan");
        provider = new ReflectionAnnotationHandlerProvider();
    }

    @After
    public void tearDown() {
        provider = null;
    }

}
