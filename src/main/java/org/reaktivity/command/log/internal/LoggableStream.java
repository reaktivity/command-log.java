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

import static java.lang.String.format;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.command.log.internal.layouts.StreamsLayout;
import org.reaktivity.command.log.internal.spy.RingBufferSpy;
import org.reaktivity.command.log.internal.types.OctetsFW;
import org.reaktivity.command.log.internal.types.stream.AbortFW;
import org.reaktivity.command.log.internal.types.stream.BeginFW;
import org.reaktivity.command.log.internal.types.stream.DataFW;
import org.reaktivity.command.log.internal.types.stream.EndFW;
import org.reaktivity.command.log.internal.types.stream.HttpBeginExFW;
import org.reaktivity.command.log.internal.types.stream.ResetFW;
import org.reaktivity.command.log.internal.types.stream.WindowFW;

import java.util.function.LongPredicate;
import java.util.function.Predicate;

public final class LoggableStream implements AutoCloseable
{
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();

    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    private final String streamFormat;
    private final String throttleFormat;
    private final String targetName;
    private final StreamsLayout layout;
    private final RingBufferSpy streamsBuffer;
    private final RingBufferSpy throttleBuffer;
    private final Logger out;
    private final boolean verbose;

    LoggableStream(
        String receiver,
        String sender,
        StreamsLayout layout,
        Logger logger,
        boolean verbose)
    {
        this.streamFormat = String.format("[%%d] [%s -> %s]\t[0x%%016x] %%s\n", sender, receiver);
        this.throttleFormat = String.format("[%%d] [%s <- %s]\t[0x%%016x] %%s\n", sender, receiver);

        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();
        this.throttleBuffer = layout.throttleBuffer();
        this.targetName = receiver;
        this.out = logger;
        this.verbose = verbose;
    }

    int process()
    {
        return streamsBuffer.spy(this::handleStream, 1) +
                throttleBuffer.spy(this::handleThrottle, 1);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    private void handleStream(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final BeginFW begin = beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = dataRO.wrap(buffer, index, index + length);
            handleData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = endRO.wrap(buffer, index, index + length);
            handleEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = abortRO.wrap(buffer, index, index + length);
            handleAbort(abort);
            break;
        }
    }

    private void handleBegin(
        final BeginFW begin)
    {
        final long timestamp = begin.timestamp();
        final long streamId = begin.streamId();
        final String sourceName = begin.source().asString();
        final long sourceRef = begin.sourceRef();
        final long correlationId = begin.correlationId();
        final long authorization = begin.authorization();

        OctetsFW extension = begin.extension();

        out.printf(streamFormat, timestamp, streamId,
                   format("BEGIN \"%s\" [0x%016x] [0x%016x] [0x%016x]", sourceName, sourceRef, correlationId, authorization));

        if (verbose && sourceName.startsWith("http"))
        {
            final boolean initial = (sourceRef != 0);
            final long typedRef = (sourceRef != 0) ? sourceRef : correlationId;
            final Predicate<String> isHttp = n -> n.startsWith("http");
            final LongPredicate isClient = r -> r > 0L && (r & 0x01L) != 0x00L;
            final LongPredicate isServer = r -> r > 0L && (r & 0x01L) == 0x00L;
            final LongPredicate isProxy = r -> r < 0L && (r & 0x01L) == 0x00L;
            final boolean isHttpClientInitial = initial && isClient.test(typedRef) && isHttp.test(targetName);
            final boolean isHttpClientReply = !initial && isClient.test(typedRef) && isHttp.test(sourceName);
            final boolean isHttpServerInitial = initial && isServer.test(typedRef) && isHttp.test(sourceName);
            final boolean isHttpServerReply = !initial && isServer.test(typedRef) && isHttp.test(targetName);
            final boolean isHttpProxyInitial = initial && isProxy.test(typedRef) && (isHttp.test(sourceName)
                    || isHttp.test(targetName));
            final boolean isHttpProxyReply = !initial && isProxy.test(typedRef) && (isHttp.test(sourceName)
                    || isHttp.test(targetName));

            if (isHttpClientInitial
                    || isHttpServerReply
                    || isHttpClientReply
                    || isHttpServerInitial
                    || isHttpProxyInitial
                    | isHttpProxyReply)
            {
                HttpBeginExFW httpBeginEx = httpBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
                httpBeginEx.headers()
                        .forEach(h -> out.printf("%s: %s\n", h.name().asString(), h.value().asString()));
            }
        }
    }

    private void handleData(
        final DataFW data)
    {
        final long timestamp = data.timestamp();
        final long streamId = data.streamId();
        final int length = data.length();
        final int padding = data.padding();
        final long authorization = data.authorization();

        out.printf(format(streamFormat, timestamp, streamId, format("DATA [%d] [%d] [0x%016x]", length, padding, authorization)));
    }

    private void handleEnd(
        final EndFW end)
    {
        final long timestamp = end.timestamp();
        final long streamId = end.streamId();
        final long authorization = end.authorization();

        out.printf(format(streamFormat, timestamp, streamId, format("END [0x%016x]", authorization)));
    }

    private void handleAbort(
        final AbortFW abort)
    {
        final long timestamp = abort.timestamp();
        final long streamId = abort.streamId();
        final long authorization = abort.authorization();

        out.printf(format(streamFormat, timestamp, streamId, format("ABORT [0x%016x]", authorization)));
    }

    private void handleThrottle(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case ResetFW.TYPE_ID:
            final ResetFW reset = resetRO.wrap(buffer, index, index + length);
            handleReset(reset);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = windowRO.wrap(buffer, index, index + length);
            handleWindow(window);
            break;
        }
    }

    private void handleReset(
        final ResetFW reset)
    {
        final long timestamp = reset.timestamp();
        final long streamId = reset.streamId();

        out.printf(format(throttleFormat, timestamp, streamId, "RESET"));
    }

    private void handleWindow(
        final WindowFW window)
    {
        final long timestamp = window.timestamp();
        final long streamId = window.streamId();
        final int credit = window.credit();
        final int padding = window.padding();
        final long groupId = window.groupId();

        out.printf(format(throttleFormat, timestamp, streamId, format("WINDOW [%d] [%d] [%d]", credit, padding, groupId)));
    }
}
