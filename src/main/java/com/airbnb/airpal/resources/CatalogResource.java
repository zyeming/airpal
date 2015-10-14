package com.airbnb.airpal.resources;

import com.airbnb.airpal.core.AirpalUser;
import com.airbnb.airpal.presto.metadata.CatalogCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.secnod.shiro.jaxrs.Auth;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Created by shivamaggarwal on 10/13/15.
 */
@Path("/api/catalog")
public class CatalogResource {
    private final CatalogCache catalogCache;

    @Inject
    public CatalogResource(final CatalogCache catalogCache)
    {
        this.catalogCache = catalogCache;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCatalogsUpdates()
    {
        final Map<String, List<String>> catalogMap = catalogCache.getCatalogMap();
        final ImmutableList.Builder<Map<String, String>> builder = ImmutableList.builder();

        for (Map.Entry<String, List<String>> entry : catalogMap.entrySet()) {
            String schema = entry.getKey();
            for (String catalog : entry.getValue()) {
                Map<String, String> map = Maps.newHashMap();
                map.put("catalog", catalog);
                builder.add(map);
            }
        }

        final ImmutableList<Map<String, String>> catalogs = builder.build();

        return Response.ok(catalogs).build();
    }
}
