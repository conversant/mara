package com.conversantmedia.mapreduce.tool.annotation.handler;

import com.conversantmedia.mapreduce.tool.ToolException;
import org.reflections.Reflections;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * A <tt>AnnotationHandlerProvider</tt> that uses reflection to scan specific packages for
 * annotation handlers, registering them automatically.
 */
public class ReflectionAnnotationHandlerProvider extends AbstractAnnotationHandlerProvider {

    /**
     * System property for overriding base packages to scan for
     * mara annotation handlers.
     */
    public static final String SYSPROP_MARA_ANNOTATION_HANDLER_SCAN_PACKAGES = "mara.annotation.handler.scan";

    @Override
    public List<MaraAnnotationHandler> getHandlers() throws ToolException {

        List<MaraAnnotationHandler> handlers = new ArrayList<MaraAnnotationHandler>();

        Reflections reflections = initReflections(getBasePackagesToScanForComponents());
        Set<Class<?>> handlerClasses = reflections.getTypesAnnotatedWith(Service.class);
        for (Class<?> handlerClass : handlerClasses) {
            if (MaraAnnotationHandler.class.isAssignableFrom(handlerClass)) {
                try {
                    MaraAnnotationHandler handler = (MaraAnnotationHandler) handlerClass.newInstance();
                    handlers.add(handler);
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new ToolException(e);
                }
            }
        }
        return handlers;
    }

    protected static String[] getBasePackagesToScanForComponents() throws ToolException {
        try {
            return MaraAnnotationUtil.INSTANCE.getBasePackagesToScan(SYSPROP_MARA_ANNOTATION_HANDLER_SCAN_PACKAGES, null);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    protected static Reflections initReflections(Object...packages) {
        return new Reflections(packages);
    }
}
