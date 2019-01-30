/**
 * Copyright 2016-2018 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.command.log.internal;

import java.util.concurrent.atomic.AtomicInteger;

import org.agrona.DirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.command.log.internal.layouts.RoutesLayout;
import org.reaktivity.command.log.internal.types.OctetsFW;
import org.reaktivity.command.log.internal.types.control.RouteFW;
import org.reaktivity.command.log.internal.types.control.TlsRouteExFW;
import org.reaktivity.command.log.internal.types.state.RouteTableFW;
import org.reaktivity.specification.http.internal.types.control.HttpRouteExFW;

public final class LoggableRoutes implements AutoCloseable
{
    private final RouteTableFW routeTableRO = new RouteTableFW();
    private final RouteFW routeRO = new RouteFW();

    private final RoutesLayout routes;
    private final Logger out;
    private final IdleStrategy idleStrategy;
    private final UnsafeBuffer routesSnapshot;
    private final LongHashSet loggedRoutes;

    LoggableRoutes(
        RoutesLayout routes,
        Logger logger,
        IdleStrategy idleStrategy)
    {
        this.routes = routes;
        this.out = logger;
        this.idleStrategy = idleStrategy;
        this.routesSnapshot = new UnsafeBuffer(new byte[routes.routesBuffer().capacity()]);
        this.loggedRoutes = new LongHashSet(-1);
    }

    int process()
    {
        final DirectBuffer routesBuffer = routes.routesBuffer();

        routesBuffer.getBytes(0, routesSnapshot, 0, routesBuffer.capacity());
        RouteTableFW routeTableSnapshot = routeTableRO.wrap(routesSnapshot, 0, routesSnapshot.capacity());
        final int modCountSnapshot = routeTableSnapshot.modificationCount();

        final RouteTableFW routeTable = routeTableRO.wrap(routesBuffer, 0, routesBuffer.capacity());
        final int modCount = routeTable.modificationCount();

        if (modCount == modCountSnapshot)
        {
            return logRoutes(routeTableSnapshot, new LongHashSet(-1), new AtomicInteger(0));
        }

        idleStrategy.idle();

        return process();
    }

    private int logRoutes(
        RouteTableFW routeTable,
        LongHashSet thisIterationRoutes,
        AtomicInteger workCnt)
    {
        routeTable.entries().forEach(e ->
        {
            final OctetsFW octets = e.route();
            final RouteFW route = routeRO.wrap(octets.buffer(), octets.offset(), octets.limit());

            final long correlationId = route.correlationId();
            final String nukleusName = route.nukleus().asString();
            final String role = route.role().toString();
            final String localAddress = route.localAddress().asString();
            final String remoteAddress = route.remoteAddress().asString();
            final long authorization = route.authorization();
            thisIterationRoutes.add(correlationId);

            if (!loggedRoutes.contains(correlationId))
            {
                workCnt.incrementAndGet();

                String extension = extension(route);
                out.printf("{" +
                           "\"$nukleus\":\"%s\", " +
                           "\"$id\":%d, " +
                           "\"role\":\"%s\", " +
                           "\"authorization\":%d, " +
                           "\"localAddress\":\"%s\", " +
                           "\"remoteAddress\":\"%s\"%s}\n",
                           nukleusName,
                           correlationId,
                           role,
                           authorization,
                           localAddress,
                           remoteAddress,
                           extension == null ? "" : String.format(", \"extension\": %s", extension));
                loggedRoutes.add(correlationId);
                workCnt.incrementAndGet();
            }
        });

        LongHashSet removedRoutes = loggedRoutes.difference(thisIterationRoutes);
        if (removedRoutes != null)
        {
            removedRoutes.stream().forEach(correlationId ->
            {
                out.printf("Unrouted %d\n", correlationId);
                loggedRoutes.remove(correlationId);
            });
        }
        return workCnt.get();
    }

    private String extension(
        RouteFW route)
    {
        final String nukleusName = route.nukleus().asString();

        String extension = null;
        if ("tls".equals(nukleusName))
        {
            TlsRouteExFW ext = new TlsRouteExFW();
            final int index = route.extension().offset();
            ext.wrap(route.extension().buffer(), index, index + route.extension().sizeof());
            final String applicationProtocol = ext.applicationProtocol().asString();
            final String hostname = ext.hostname().asString();
            final String store = ext.store().asString();
            extension = String.format(
            "{" +
            "\"store\":%s," +
            "\"hostname\":%s," +
            "\"applicationProtocol\":%s" +
            "}",
            store != null ? String.format("\"%s\"", store) : null,
            hostname != null ? String.format("\"%s\"", hostname) : null,
            applicationProtocol != null ? String.format("\"%s\"", applicationProtocol) : null);
        }
        else if ("http2".equals(nukleusName))
        {
            HttpRouteExFW httpExt = new HttpRouteExFW();
            route.extension().get(httpExt::wrap);
            StringBuilder headers = new StringBuilder();
            httpExt.headers().forEach(h -> headers.append(String.format("\"%s\":\"%s\",",
                    h.name().asString(), h.value().asString())));
            extension = String.format(
                    "{" +
                    "\"headers\": {%s}" +
                    "}", headers.deleteCharAt(headers.length()-1));
        }

        return extension;
    }

    @Override
    public void close() throws Exception
    {
        routes.close();
    }
}
