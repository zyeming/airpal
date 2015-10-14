package com.airbnb.airpal.presto.metadata;

import com.airbnb.airpal.core.BackgroundCacheLoader;
import com.airbnb.airpal.core.execution.QueryClient;
import com.airbnb.airpal.presto.QueryRunner;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by shivamaggarwal on 10/13/15.
 */

@Slf4j
public class CatalogCache implements Closeable {

    private static final long RELOAD_TIME_MINUTES = 2;
    private static final Set<String> EXCLUDED_CATALOGS = Sets.newHashSet("system");

    private final ExecutorService executor;
    private final QueryRunner.QueryRunnerFactory queryRunnerFactory;
    private final LoadingCache<String, List<String>> catalogTableCache;

    public CatalogCache(final QueryRunner.QueryRunnerFactory queryRunnerFactory,
                       final ExecutorService executor)
    {
        this.queryRunnerFactory = checkNotNull(queryRunnerFactory, "queryRunnerFactory session was null!");
        this.executor = checkNotNull(executor, "executor was null!");

        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);
        BackgroundCacheLoader<String, List<String>> loader =
                new BackgroundCacheLoader<String, List<String>>(listeningExecutor)
                {
                    @Override
                    public List<String> load(String catalogs)
                    {
                        return queryMetadata("SHOW CATALOGS");
                    }
                };

        catalogTableCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(RELOAD_TIME_MINUTES, TimeUnit.MINUTES)
                .build(loader);
    }

    private List<String> queryMetadata(String query)
    {
        final ImmutableList.Builder<String> cache = ImmutableList.builder();
        QueryRunner queryRunner = queryRunnerFactory.create();
        QueryClient queryClient = new QueryClient(queryRunner, io.dropwizard.util.Duration.seconds(60), query);

        try {
            queryClient.executeWith(new Function<StatementClient, Void>() {
                @Nullable
                @Override
                public Void apply(StatementClient client)
                {
                    QueryResults results = client.current();
                    if (results.getData() != null) {
                        for (List<Object> row : results.getData()) {
                            String catalog = (String) row.get(0);

                            if (EXCLUDED_CATALOGS.contains(catalog)) {
                                continue;
                            }
                            cache.add(catalog);
                        }
                    }

                    return null;
                }
            });
        }
        catch (QueryClient.QueryTimeOutException e) {
            log.error("Caught timeout loading columns", e);
        }

        return cache.build();
    }

    public void populateCache(final String catalog)
    {
        checkNotNull(catalog, "catalog list is null");
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                catalogTableCache.refresh(catalog);
            }
        });
    }

    public Map<String, List<String>> getCatalogMap()
    {
        try {
            return catalogTableCache.asMap();
        }
        catch (Exception e) {
            e.printStackTrace();
            return Maps.newHashMap();
        }
    }

    public static ThreadFactory daemonThreadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
    }

    @Override
    public void close() throws IOException {
        executor.shutdownNow();
    }
}
