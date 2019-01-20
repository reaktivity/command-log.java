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

import java.nio.file.Path;

import org.agrona.concurrent.status.CountersManager;
import org.reaktivity.command.log.internal.layouts.ControlLayout;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;

public final class LogCountersCommand implements Runnable
{
    private final Path directory;
    private final boolean separator;
    private final Logger out;
    private final CountersManager countersManager;

    LogCountersCommand(
        ReaktorConfiguration config,
        Logger out,
        boolean separator)
    {
        this.directory = config.directory();
        this.separator = separator;
        this.out = out;

        ControlLayout layout = new ControlLayout.Builder()
                .controlPath(directory.resolve("control"))
                .readonly(true)
                .build();

        this.countersManager = new CountersManager(layout.counterLabelsBuffer(), layout.counterValuesBuffer());
    }

    @Override
    public void run()
    {
        final String valueFormat = separator ? ",d" : "d";

        countersManager.forEach((id, name) -> out.printf(
                "{" +
                "\"name\": \"%s\"," +
                "\"value\":%" + valueFormat +
                "}\n", name, countersManager.getCounterValue(id)));
        out.printf("\n");
    }
}
