/**
 * Copyright 2016-2021 The Reaktivity Project
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.file.Path;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.reaktivity.command.log.internal.layouts.RoutesLayout;
import org.reaktivity.nukleus.Configuration;

public class LogRoutesCommand implements Runnable
{
    private static final long MAX_PARK_NS = MILLISECONDS.toNanos(100L);
    private static final long MIN_PARK_NS = MILLISECONDS.toNanos(1L);
    private static final int MAX_YIELDS = 300;
    private static final int MAX_SPINS = 200;

    private final LoggableRoutes logger;
    private final Logger out;

    LogRoutesCommand(
        Configuration config,
        Logger out,
        boolean verbose)
    {
        final Path directory = config.directory();
        final RoutesLayout layout = new RoutesLayout.Builder()
                .routesPath(directory.resolve("routes"))
                .build();
        final IdleStrategy idleStrategy = new BackoffIdleStrategy(MAX_SPINS, MAX_YIELDS, MIN_PARK_NS, MAX_PARK_NS);

        this.logger = new LoggableRoutes(layout, out, idleStrategy);
        this.out = out;
    }

    @Override
    public void run()
    {
        logger.process();
        out.printf("\n");
    }
}
