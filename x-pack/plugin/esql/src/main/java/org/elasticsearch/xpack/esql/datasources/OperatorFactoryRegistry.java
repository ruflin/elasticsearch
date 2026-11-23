/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorFactoryProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Registry for source operator factories.
 *
 * <p>This registry provides a single entry point for creating source operator factories.
 * It supports two modes:
 * <ol>
 *   <li><b>ExternalSourceFactory</b>: Unified factories for connectors, table catalogs,
 *       and file-based sources. ConnectorFactory instances are dispatched via the connector
 *       protocol; file-based sources go through {@code FileSourceFactory.operatorFactory()};
 *       other factories use their {@code operatorFactory()} capability.</li>
 *   <li><b>Plugin factories</b>: Backward-compat bridge for {@code DataSourcePlugin.operatorFactories()}.</li>
 * </ol>
 */
public class OperatorFactoryRegistry {

    private final Map<String, ExternalSourceFactory> sourceFactories;
    // FIXME: pluginFactories is a backward-compat bridge for DataSourcePlugin.operatorFactories().
    // Once plugins migrate to ExternalSourceFactory.operatorFactory(), remove this field.
    private final Map<String, SourceOperatorFactoryProvider> pluginFactories;
    private final Executor executor;
    private final Executor fileReadExecutor;

    public OperatorFactoryRegistry(
        Map<String, ExternalSourceFactory> sourceFactories,
        Map<String, SourceOperatorFactoryProvider> pluginFactories,
        Executor executor
    ) {
        this(sourceFactories, pluginFactories, executor, executor);
    }

    public OperatorFactoryRegistry(
        Map<String, ExternalSourceFactory> sourceFactories,
        Map<String, SourceOperatorFactoryProvider> pluginFactories,
        Executor executor,
        Executor fileReadExecutor
    ) {
        Check.notNull(executor, "executor cannot be null");
        Check.notNull(fileReadExecutor, "fileReadExecutor cannot be null");
        this.sourceFactories = sourceFactories != null ? Map.copyOf(sourceFactories) : Map.of();
        this.pluginFactories = pluginFactories != null ? Map.copyOf(pluginFactories) : Map.of();
        this.executor = executor;
        this.fileReadExecutor = fileReadExecutor;
    }

    public SourceOperator.SourceOperatorFactory factory(SourceOperatorContext context) {
        String sourceType = context.sourceType();

        ExternalSourceFactory sf = sourceType != null ? sourceFactories.get(sourceType) : null;
        if (sf != null) {
            if (sf instanceof ConnectorFactory cf) {
                Connector connector = cf.open(context.path(), context.config());
                List<String> projectedColumns = new ArrayList<>(context.attributes().size());
                for (Attribute attr : context.attributes()) {
                    projectedColumns.add(attr.name());
                }
                String target = cf.resolveTarget(context.path(), context.config());
                QueryRequest request = QueryRequest.forConnector(target, projectedColumns, context);
                return new AsyncConnectorSourceOperatorFactory(connector, request, context.maxBufferSize(), executor, context.sliceQueue());
            }
            SourceOperatorFactoryProvider opFactory = sf.operatorFactory();
            if (opFactory != null) {
                return opFactory.create(context);
            }
        }

        // FIXME: backward-compat bridge for standalone pluginFactories
        if (sourceType != null && pluginFactories.containsKey(sourceType)) {
            return pluginFactories.get(sourceType).create(context);
        }

        throw new IllegalArgumentException("No operator factory for sourceType: " + sourceType);
    }

    public boolean hasPluginFactory(String sourceType) {
        return sourceType != null && (sourceFactories.containsKey(sourceType) || pluginFactories.containsKey(sourceType));
    }

    /**
     * Whether a source type resolves to the built-in file factory used for registered datasets.
     */
    public boolean isFileSourceFactory(String sourceType) {
        return sourceType != null && sourceFactories.get(sourceType) instanceof FileSourceFactory;
    }

    /**
     * Whether a registered dataset with the given {@code sourceType} and rebound {@code sourcePath}
     * may be read through this registry. Allows the built-in file factory, or a {@link ConnectorFactory}
     * that explicitly opts in to registered datasets and handles {@code sourcePath} (preventing a
     * crafted plan from pairing a file dataset's credentials with a connector).
     */
    public boolean supportsRegisteredDatasets(String sourceType, String sourcePath) {
        if (sourceType == null) {
            return false;
        }
        ExternalSourceFactory sf = sourceFactories.get(sourceType);
        if (sf instanceof FileSourceFactory) {
            return true;
        }
        return sf instanceof ConnectorFactory cf && cf.supportsRegisteredDatasets() && cf.canHandle(sourcePath);
    }

    public Map<String, ExternalSourceFactory> sourceFactories() {
        return sourceFactories;
    }

    public Executor executor() {
        return executor;
    }

    /**
     * Thread pool for file (and similar) background reads. Isolated from {@link #executor} (typically
     * {@code esql_worker}, which also runs compute drivers) so blocked producers in buffer backpressure
     * cannot starve the drivers that free buffer space.
     */
    public Executor fileReadExecutor() {
        return fileReadExecutor;
    }
}
