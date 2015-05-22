package com.conversantmedia.mapreduce.tool.annotation.handler;

import com.conversantmedia.mapreduce.tool.ToolException;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Created by pjaromin on 5/21/2015.
 */
public abstract class AbstractAnnotationHandlerProvider implements AnnotationHandlerProvider {

    // List of handlers to skip - used to, for example, skip processing specific default
    // handlers. Alternatively makes it easier to replace one with your own implementation.
    public static final String SYSPROP_SKIP_HANDLERS = "mara.skip.annotation.handlers";

    public abstract List<MaraAnnotationHandler> getHandlers() throws ToolException;

    @Override
    public Iterable<MaraAnnotationHandler> handlers() throws ToolException {

        List<MaraAnnotationHandler> handlers = getHandlers();

        // Remove any that we should skip...
        Set<String> skip = getHandlersToSkip();
        if (skip != null) {
            Iterator<MaraAnnotationHandler> iter = handlers.iterator();
            while (iter.hasNext()) {
                MaraAnnotationHandler handler = iter.next();
                if (skip.contains(handler.getClass().getName())) {
                    iter.remove();
                }
            }
        }

        // Honor the 'runLast' flag
        handlers.sort(new Comparator<MaraAnnotationHandler>() {
            @Override
            public int compare(MaraAnnotationHandler o1, MaraAnnotationHandler o2) {
                if (o1.runLast() == o2.runLast()) return 0;
                if (o1.runLast()) return 1;
                return -1;
            }
        });
        return handlers;
    }

    public Set<String> getHandlersToSkip() {
        String skipStr = System.getProperty(SYSPROP_SKIP_HANDLERS);
        if (StringUtils.isNotBlank(skipStr)) {
            String[] handlers = StringUtils.split(skipStr, ",");
            Set<String> handlerSet = new HashSet<String>();
            handlerSet.addAll(Arrays.asList(handlers));
            return handlerSet;
        }
        return null;
    }
}
