/**
 * Copyright 2016-2017 The Reaktivity Project
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

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.reaktivity.command.log.internal.layouts.StreamsLayout;
import org.reaktivity.nukleus.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.HEAD_POSITION_OFFSET;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TAIL_POSITION_OFFSET;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public final class LogBufferCapacityCommand
{
    private final Path directory;
    private final boolean verbose;
    private final Logger out;

    private final long streamsCapacity;
    private final long throttleCapacity;

    private final String logformat = "%s readPointer=%d writePointer=%d capacity=%d";

    public LogBufferCapacityCommand(
            Configuration config,
            Logger out,
            boolean verbose)
    {
        this.directory = config.directory();
        this.out = out;
        this.verbose = verbose;
        this.streamsCapacity = config.streamsBufferCapacity();
        this.throttleCapacity = config.throttleBufferCapacity();
    }

    private boolean isStreamsFile(
            Path path)
    {
        return path.getNameCount() - directory.getNameCount() == 3 &&
                "streams".equals(path.getName(path.getNameCount() - 2).toString()) &&
                Files.isRegularFile(path);
    }

    private void onDiscovered(
            Path path)
    {
        if (verbose)
        {
            out.printf("Discovered: %s\n", path);
        }
    }

    private void bufferCapacity(
            Path path)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
                .path(path)
                .streamsCapacity(streamsCapacity)
                .throttleCapacity(throttleCapacity)
                .readonly(true)
                .build();
        DirectBuffer buffer = layout.streamsBuffer().buffer();
        System.out.println(String.format(
                logformat,
                directory.relativize(path),
                buffer.getLong(buffer.capacity()  - TRAILER_LENGTH + HEAD_POSITION_OFFSET),
                buffer.getLong(buffer.capacity() - TRAILER_LENGTH + TAIL_POSITION_OFFSET),
                buffer.capacity()));
    }

    void invoke()
    {
        try (Stream<Path> files = Files.walk(directory, 3))
        {
            files.filter(this::isStreamsFile)
                    .peek(this::onDiscovered)
                    .forEach(this::bufferCapacity);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
