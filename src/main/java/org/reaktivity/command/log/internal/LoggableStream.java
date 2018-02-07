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

import java.util.function.LongPredicate;
import java.util.function.Predicate;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.reaktivity.command.log.internal.layouts.StreamsLayout;
import org.reaktivity.command.log.internal.spy.RingBufferSpy;
import org.reaktivity.command.log.internal.types.ListFW;
import org.reaktivity.command.log.internal.types.OctetsFW;
import org.reaktivity.command.log.internal.types.stream.AckFW;
import org.reaktivity.command.log.internal.types.stream.BeginFW;
import org.reaktivity.command.log.internal.types.stream.HttpBeginExFW;
import org.reaktivity.command.log.internal.types.stream.RegionFW;
import org.reaktivity.command.log.internal.types.stream.TransferFW;

public final class LoggableStream implements AutoCloseable
{
    private static final String BEGIN_FORMAT    = "BEGIN    [0x%08x] [0x%016x] [0x%016x] [0x%016x] \"%s\"";
    private static final String TRANSFER_FORMAT = "TRANSFER [0x%08x] [%d] [0x%016x]";
    private static final String ACK_FORMAT      = "ACK      [0x%08x] [%d]";

    private final BeginFW beginRO = new BeginFW();
    private final TransferFW transferRO = new TransferFW();
    private final AckFW ackRO = new AckFW();

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
        this.streamFormat = String.format("[%s -> %s]\t[0x%%016x] %%s\n", sender, receiver);
        this.throttleFormat = String.format("[%s <- %s]\t[0x%%016x] %%s\n", sender, receiver);

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
        case TransferFW.TYPE_ID:
            final TransferFW transfer = transferRO.wrap(buffer, index, index + length);
            handleTransfer(transfer);
            break;
        }
    }

    private void handleBegin(
        final BeginFW begin)
    {
        final long streamId = begin.streamId();
        final String sourceName = begin.source().asString();
        final long sourceRef = begin.sourceRef();
        final long correlationId = begin.correlationId();
        final long authorization = begin.authorization();
        final int flags = begin.flags();

        OctetsFW extension = begin.extension();

        out.printf(streamFormat, streamId, format(BEGIN_FORMAT, flags, sourceRef, correlationId, authorization, sourceName));

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

    private void handleTransfer(
        final TransferFW transfer)
    {
        final long streamId = transfer.streamId();
        final long authorization = transfer.authorization();
        final int flags = transfer.flags();
        final ListFW<RegionFW> regions = transfer.regions();

        final MutableInteger length = new MutableInteger();
        regions.forEach(r -> length.value += r.length());

        out.printf(format(streamFormat, streamId, format(TRANSFER_FORMAT, flags, length.value, authorization)));
    }

    private void handleThrottle(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case AckFW.TYPE_ID:
            final AckFW ack = ackRO.wrap(buffer, index, index + length);
            handleAck(ack);
            break;
        }
    }

    private void handleAck(
        final AckFW ack)
    {
        final long streamId = ack.streamId();
        final int flags = ack.flags();
        final ListFW<RegionFW> regions = ack.regions();

        final MutableInteger length = new MutableInteger();
        regions.forEach(r -> length.value += r.length());

        out.printf(format(throttleFormat, streamId, format(ACK_FORMAT, flags, length.value)));
    }
}
