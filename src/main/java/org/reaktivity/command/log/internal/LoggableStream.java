/**
 * Copyright 2016-2019 The Reaktivity Project
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
import static java.net.InetAddress.getByAddress;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.command.log.internal.labels.LabelManager;
import org.reaktivity.command.log.internal.layouts.StreamsLayout;
import org.reaktivity.command.log.internal.spy.RingBufferSpy;
import org.reaktivity.command.log.internal.types.OctetsFW;
import org.reaktivity.command.log.internal.types.TcpAddressFW;
import org.reaktivity.command.log.internal.types.control.Role;
import org.reaktivity.command.log.internal.types.stream.AbortFW;
import org.reaktivity.command.log.internal.types.stream.BeginFW;
import org.reaktivity.command.log.internal.types.stream.DataFW;
import org.reaktivity.command.log.internal.types.stream.EndFW;
import org.reaktivity.command.log.internal.types.stream.FrameFW;
import org.reaktivity.command.log.internal.types.stream.HttpBeginExFW;
import org.reaktivity.command.log.internal.types.stream.ResetFW;
import org.reaktivity.command.log.internal.types.stream.SignalFW;
import org.reaktivity.command.log.internal.types.stream.TcpBeginExFW;
import org.reaktivity.command.log.internal.types.stream.WindowFW;

public final class LoggableStream implements AutoCloseable
{
    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final SignalFW signalRO = new SignalFW();

    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();

    private final TcpBeginExFW tcpBeginExRO = new TcpBeginExFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    private final int index;
    private final LabelManager labels;
    private final String streamFormat;
    private final String throttleFormat;
    private final StreamsLayout layout;
    private final RingBufferSpy streamsBuffer;
    private final Logger out;
    private final boolean verbose;
    private final Long2LongHashMap budgets;
    private final Long2LongHashMap timestamps;
    private final LongPredicate nextTimestamp;

    LoggableStream(
        int index,
        LabelManager labels,
        Long2LongHashMap budgets,
        StreamsLayout layout,
        Logger logger,
        boolean verbose,
        Long2LongHashMap timestamps,
        LongPredicate nextTimestamp)
    {
        this.index = index;
        this.labels = labels;
        this.streamFormat = "[%d] [0x%08x] [0x%016x] [%s -> %s]\t[0x%016x] [0x%016x] [%016x] %s\n";
        this.throttleFormat = "[%d] [0x%08x] [0x%016x] [%s <- %s]\t[0x%016x] [0x%016x] [%016x] %s\n";

        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();
        this.out = logger;
        this.verbose = verbose;
        this.budgets = budgets;
        this.timestamps = timestamps;
        this.nextTimestamp = nextTimestamp;
    }

    int process()
    {
        return streamsBuffer.spy(this::handleFrame, 1);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    @Override
    public String toString()
    {
        return String.format("data%d (spy)", index);
    }

    private boolean handleFrame(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        final FrameFW frame = frameRO.wrap(buffer, index, index + length);
        final long timestamp = frame.timestamp();

        if (!nextTimestamp.test(timestamp))
        {
            return false;
        }

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
        case SignalFW.TYPE_ID:
            final SignalFW signal = signalRO.wrap(buffer, index, index + length);
            handleSignal(signal);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = resetRO.wrap(buffer, index, index + length);
            handleReset(reset);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = windowRO.wrap(buffer, index, index + length);
            handleWindow(window);
            break;
        }

        return true;
    }

    private void handleBegin(
        final BeginFW begin)
    {
        final long timestamp = begin.timestamp();
        final long routeId = begin.routeId();
        final long streamId = begin.streamId();
        final long traceId = begin.trace();
        final long authorization = begin.authorization();
        final int budget = (int) budgets.computeIfAbsent(streamId, id -> 0L);
        final long initialId = streamId | 0x0000_0000_0000_0001L;
        final long timeStart = timestamps.computeIfAbsent(initialId, id -> timestamp);
        final long timeOffset = timeStart != -1L ? timestamp - timeStart : -1L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, timestamp, budget, traceId, sourceName, targetName, routeId, streamId, timeOffset,
                   format("BEGIN [0x%016x]", authorization));

        OctetsFW extension = begin.extension();
        if (verbose && extension.sizeof() != 0)
        {
            if (sourceName.startsWith("tcp") || targetName.startsWith("tcp"))
            {
                TcpBeginExFW tcpBeginEx = tcpBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
                InetSocketAddress localAddress = toInetSocketAddress(tcpBeginEx.localAddress(), tcpBeginEx.localPort());
                InetSocketAddress remoteAddress = toInetSocketAddress(tcpBeginEx.remoteAddress(), tcpBeginEx.remotePort());
                out.printf("[%d] %s\t%s\n", timestamp, localAddress, remoteAddress);
            }

            if (sourceName.startsWith("http") || targetName.startsWith("http"))
            {
                final int roleId = (int)(routeId >> 28) & 0x0f;
                if (roleId != 0x0f)
                {
                    final Role role = Role.valueOf(roleId);
                    final boolean isInitial = (streamId & 0x0000_0000_0000_0001L) != 0;
                    final boolean isReply = (streamId & 0x0000_0000_0000_0001L) == 0;
                    Predicate<String> isHttp11 = label -> label.startsWith("http#");
                    Predicate<String> isHttp2 = label -> label.startsWith("http2#");
                    Predicate<String> isHttpCodec = isHttp11.or(isHttp2);

                    if (!(role == Role.SERVER && isInitial && isHttpCodec.test(targetName)) &&
                        !(role == Role.SERVER && isReply && isHttpCodec.test(sourceName)) &&
                        !(role == Role.CLIENT && isInitial && isHttpCodec.test(sourceName)) &&
                        !(role == Role.CLIENT && isReply && isHttpCodec.test(targetName)))
                    {
                        HttpBeginExFW httpBeginEx = httpBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
                        httpBeginEx.headers()
                                .forEach(h -> out.printf("[%d] %s: %s\n", timestamp, h.name().asString(), h.value().asString()));
                    }
                }
            }
        }
    }

    private void handleData(
        final DataFW data)
    {
        final long timestamp = data.timestamp();
        final long routeId = data.routeId();
        final long streamId = data.streamId();
        final long traceId = data.trace();
        final int length = data.length();
        final int padding = data.padding();
        final long authorization = data.authorization();
        final byte flags = (byte) (data.flags() & 0xFF);
        final int budget = (int) (long) budgets.computeIfPresent(streamId, (i, b) -> b - (length + padding));
        final long initialId = streamId | 0x0000_0000_0000_0001L;
        final long timeStart = timestamps.get(initialId);
        final long timeOffset = timeStart != -1L ? timestamp - timeStart : -1L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, timestamp, budget, traceId, sourceName, targetName, routeId, streamId, timeOffset,
                      format("DATA [%d] [%d] [%x] [0x%016x]", length, padding, flags, authorization));
    }

    private void handleEnd(
        final EndFW end)
    {
        final long timestamp = end.timestamp();
        final long routeId = end.routeId();
        final long streamId = end.streamId();
        final long traceId = end.trace();
        final long authorization = end.authorization();
        final int budget = (int) budgets.get(streamId);
        final long initialId = streamId | 0x0000_0000_0000_0001L;
        final long timeStart = timestamps.get(initialId);
        final long timeOffset = timeStart != -1L ? timestamp - timeStart : -1L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, timestamp, budget, traceId, sourceName, targetName, routeId, streamId, timeOffset,
                format("END [0x%016x]", authorization));
    }

    private void handleAbort(
        final AbortFW abort)
    {
        final long timestamp = abort.timestamp();
        final long routeId = abort.routeId();
        final long streamId = abort.streamId();
        final long traceId = abort.trace();
        final long authorization = abort.authorization();
        final int budget = (int) budgets.get(streamId);
        final long initialId = streamId | 0x0000_0000_0000_0001L;
        final long timeStart = timestamps.get(initialId);
        final long timeOffset = timeStart != -1L ? timestamp - timeStart : -1L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, timestamp, budget, traceId, sourceName, targetName, routeId, streamId, timeOffset,
                format("ABORT [0x%016x]", authorization));
    }

    private void handleSignal(
        final SignalFW signal)
    {
        final long timestamp = signal.timestamp();
        final long routeId = signal.routeId();
        final long streamId = signal.streamId();
        final long traceId = signal.trace();
        final long authorization = signal.authorization();
        final long signalId = signal.signalId();
        final int budget = (int) budgets.get(streamId);
        final long initialId = streamId | 0x0000_0000_0000_0001L;
        final long timeStart = timestamps.get(initialId);
        final long timeOffset = timeStart != -1L ? timestamp - timeStart : -1L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, timestamp, budget, traceId, sourceName, targetName, routeId, streamId, timeOffset,
                format("SIGNAL [%d] [0x%016x]", signalId, authorization));
    }

    private void handleReset(
        final ResetFW reset)
    {
        final long timestamp = reset.timestamp();
        final long routeId = reset.routeId();
        final long streamId = reset.streamId();
        final long traceId = reset.trace();
        final int budget = (int) budgets.get(streamId);
        final long initialId = streamId | 0x0000_0000_0000_0001L;
        final long timeStart = timestamps.get(initialId);
        final long timeOffset = timeStart != -1L ? timestamp - timeStart : -1L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(throttleFormat, timestamp, budget, traceId, sourceName, targetName, routeId, streamId, timeOffset,
                "RESET");
    }

    private void handleWindow(
        final WindowFW window)
    {
        final long timestamp = window.timestamp();
        final long routeId = window.routeId();
        final long streamId = window.streamId();
        final long traceId = window.trace();
        final int credit = window.credit();
        final int padding = window.padding();
        final long groupId = window.groupId();
        final int budget = (int) (long) budgets.computeIfPresent(streamId, (i, b) -> b + credit);
        final long initialId = streamId | 0x0000_0000_0000_0001L;
        final long timeStart = timestamps.get(initialId);
        final long timeOffset = timeStart != -1L ? timestamp - timeStart : -1L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(throttleFormat, timestamp, budget, traceId, sourceName, targetName, routeId, streamId, timeOffset,
                format("WINDOW [%d] [%d] [%d]", credit, padding, groupId));
    }

    private InetSocketAddress toInetSocketAddress(
        TcpAddressFW tcpAddress,
        int tcpPort)
    {
        InetSocketAddress socketAddress = null;

        try
        {
            byte[] address;

            switch (tcpAddress.kind())
            {
            case TcpAddressFW.KIND_IPV4_ADDRESS:
                address = new byte[4];
                tcpAddress.ipv4Address().get((b, o, l) ->
                {
                    b.getBytes(o, address); return address;
                });
                socketAddress = new InetSocketAddress(getByAddress(address), tcpPort);
                break;
            case TcpAddressFW.KIND_IPV6_ADDRESS:
                address = new byte[16];
                tcpAddress.ipv4Address().get((b, o, l) ->
                {
                    b.getBytes(o, address); return address;
                });
                socketAddress = new InetSocketAddress(getByAddress(address), tcpPort);
                break;
            case TcpAddressFW.KIND_HOST:
                String hostName = tcpAddress.host().asString();
                socketAddress = new InetSocketAddress(hostName, tcpPort);
                break;
            }
        }
        catch (UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return socketAddress;
    }
}
