/**
 * Copyright 2016-2020 The Reaktivity Project
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
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.HEADER_LENGTH;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.function.Consumer;
import java.util.function.LongPredicate;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.reaktivity.command.log.internal.labels.LabelManager;
import org.reaktivity.command.log.internal.layouts.StreamsLayout;
import org.reaktivity.command.log.internal.spy.RingBufferSpy;
import org.reaktivity.command.log.internal.types.ArrayFW;
import org.reaktivity.command.log.internal.types.KafkaConfigFW;
import org.reaktivity.command.log.internal.types.KafkaHeaderFW;
import org.reaktivity.command.log.internal.types.KafkaKeyFW;
import org.reaktivity.command.log.internal.types.KafkaOffsetFW;
import org.reaktivity.command.log.internal.types.KafkaPartitionFW;
import org.reaktivity.command.log.internal.types.OctetsFW;
import org.reaktivity.command.log.internal.types.String16FW;
import org.reaktivity.command.log.internal.types.StringFW;
import org.reaktivity.command.log.internal.types.TcpAddressFW;
import org.reaktivity.command.log.internal.types.stream.AbortFW;
import org.reaktivity.command.log.internal.types.stream.BeginFW;
import org.reaktivity.command.log.internal.types.stream.ChallengeFW;
import org.reaktivity.command.log.internal.types.stream.DataFW;
import org.reaktivity.command.log.internal.types.stream.EndFW;
import org.reaktivity.command.log.internal.types.stream.ExtensionFW;
import org.reaktivity.command.log.internal.types.stream.FlushFW;
import org.reaktivity.command.log.internal.types.stream.FrameFW;
import org.reaktivity.command.log.internal.types.stream.HttpBeginExFW;
import org.reaktivity.command.log.internal.types.stream.HttpDataExFW;
import org.reaktivity.command.log.internal.types.stream.HttpEndExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaDescribeBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaDescribeDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaFetchBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaFetchDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMergedBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMergedDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMetaBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMetaDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaProduceBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaProduceDataExFW;
import org.reaktivity.command.log.internal.types.stream.ResetFW;
import org.reaktivity.command.log.internal.types.stream.SignalFW;
import org.reaktivity.command.log.internal.types.stream.TcpBeginExFW;
import org.reaktivity.command.log.internal.types.stream.TlsBeginExFW;
import org.reaktivity.command.log.internal.types.stream.WindowFW;

public final class LoggableStream implements AutoCloseable
{
    private final FrameFW frameRO = new FrameFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();
    private final SignalFW signalRO = new SignalFW();
    private final ChallengeFW challengeRO = new ChallengeFW();
    private final FlushFW flushRO = new FlushFW();

    private final ExtensionFW extensionRO = new ExtensionFW();

    private final TcpBeginExFW tcpBeginExRO = new TcpBeginExFW();
    private final TlsBeginExFW tlsBeginExRO = new TlsBeginExFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final HttpDataExFW httpDataExRO = new HttpDataExFW();
    private final HttpEndExFW httpEndExRO = new HttpEndExFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();
    private final KafkaDataExFW kafkaDataExRO = new KafkaDataExFW();

    private final int index;
    private final LabelManager labels;
    private final String streamFormat;
    private final String throttleFormat;
    private final String verboseFormat;
    private final StreamsLayout layout;
    private final RingBufferSpy streamsBuffer;
    private final Logger out;
    private final boolean verbose;
    private final LongPredicate nextTimestamp;

    private final Int2ObjectHashMap<Consumer<BeginFW>> beginHandlers;
    private final Int2ObjectHashMap<Consumer<EndFW>> endHandlers;
    private final Int2ObjectHashMap<Consumer<DataFW>> dataHandlers;

    LoggableStream(
        int index,
        LabelManager labels,
        StreamsLayout layout,
        Logger logger,
        boolean verbose,
        LongPredicate nextTimestamp)
    {
        this.index = index;
        this.labels = labels;
        this.streamFormat = "[%02d/%08x] [0x%016x] [0x%016x] [%s -> %s]\t[0x%016x] [0x%016x] %s\n";
        this.throttleFormat = "[%02d/%08x] [0x%016x] [0x%016x] [%s <- %s]\t[0x%016x] [0x%016x] %s\n";
        this.verboseFormat = "[%02d/%08x] [0x%016x] %s\n";

        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();
        this.out = logger;
        this.verbose = verbose;
        this.nextTimestamp = nextTimestamp;

        this.beginHandlers = new Int2ObjectHashMap<>();
        this.beginHandlers.put(labels.lookupLabelId("tcp"), this::onTcpBeginEx);
        this.beginHandlers.put(labels.lookupLabelId("tls"), this::onTlsBeginEx);
        this.beginHandlers.put(labels.lookupLabelId("http"), this::onHttpBeginEx);
        this.beginHandlers.put(labels.lookupLabelId("kafka"), this::onKafkaBeginEx);

        this.dataHandlers = new Int2ObjectHashMap<>();
        this.dataHandlers.put(labels.lookupLabelId("http"), this::onHttpDataEx);
        this.dataHandlers.put(labels.lookupLabelId("kafka"), this::onKafkaDataEx);

        this.endHandlers = new Int2ObjectHashMap<>();
        this.endHandlers.put(labels.lookupLabelId("http"), this::onHttpEndEx);
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
            onBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = dataRO.wrap(buffer, index, index + length);
            onData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = endRO.wrap(buffer, index, index + length);
            onEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = abortRO.wrap(buffer, index, index + length);
            onAbort(abort);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = resetRO.wrap(buffer, index, index + length);
            onReset(reset);
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = windowRO.wrap(buffer, index, index + length);
            onWindow(window);
            break;
        case SignalFW.TYPE_ID:
            final SignalFW signal = signalRO.wrap(buffer, index, index + length);
            onSignal(signal);
            break;
        case ChallengeFW.TYPE_ID:
            final ChallengeFW challenge = challengeRO.wrap(buffer, index, index + length);
            onChallenge(challenge);
            break;
        case FlushFW.TYPE_ID:
            final FlushFW flush = flushRO.wrap(buffer, index, index + length);
            onFlush(flush);
            break;
        }

        return true;
    }

    private void onBegin(
        final BeginFW begin)
    {
        final int offset = begin.offset() - HEADER_LENGTH;
        final long timestamp = begin.timestamp();
        final long routeId = begin.routeId();
        final long streamId = begin.streamId();
        final long traceId = begin.traceId();
        final long authorization = begin.authorization();
        final long affinity = begin.affinity();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                   format("BEGIN [0x%016x] [0x%016x]", authorization, affinity));

        if (verbose)
        {
            final ExtensionFW extension = begin.extension().get(extensionRO::tryWrap);
            if (extension != null)
            {
                final Consumer<BeginFW> beginHandler = beginHandlers.get(extension.typeId());
                if (beginHandler != null)
                {
                    beginHandler.accept(begin);
                }
            }
        }
    }

    private void onData(
        final DataFW data)
    {
        final int offset = data.offset() - HEADER_LENGTH;
        final long timestamp = data.timestamp();
        final long routeId = data.routeId();
        final long streamId = data.streamId();
        final long traceId = data.traceId();
        final long budgetId = data.budgetId();
        final int length = data.length();
        final int reserved = data.reserved();
        final long authorization = data.authorization();
        final byte flags = (byte) (data.flags() & 0xFF);
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                      format("DATA [0x%016x] [%d] [%d] [%x] [0x%016x]", budgetId, length, reserved, flags, authorization));
        if (verbose)
        {
            final ExtensionFW extension = data.extension().get(extensionRO::tryWrap);
            if (extension != null)
            {
                final Consumer<DataFW> dataHandler = dataHandlers.get(extension.typeId());
                if (dataHandler != null)
                {
                    dataHandler.accept(data);
                }
            }
        }
    }

    private void onEnd(
        final EndFW end)
    {
        final int offset = end.offset() - HEADER_LENGTH;
        final long timestamp = end.timestamp();
        final long routeId = end.routeId();
        final long streamId = end.streamId();
        final long traceId = end.traceId();
        final long authorization = end.authorization();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                format("END [0x%016x]", authorization));

        if (verbose)
        {
            final ExtensionFW extension = end.extension().get(extensionRO::tryWrap);
            if (extension != null)
            {
                final Consumer<EndFW> endHandler = endHandlers.get(extension.typeId());
                if (endHandler != null)
                {
                    endHandler.accept(end);
                }
            }
        }
    }

    private void onAbort(
        final AbortFW abort)
    {
        final int offset = abort.offset() - HEADER_LENGTH;
        final long timestamp = abort.timestamp();
        final long routeId = abort.routeId();
        final long streamId = abort.streamId();
        final long traceId = abort.traceId();
        final long authorization = abort.authorization();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(streamFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                format("ABORT [0x%016x]", authorization));
    }

    private void onReset(
        final ResetFW reset)
    {
        final int offset = reset.offset() - HEADER_LENGTH;
        final long timestamp = reset.timestamp();
        final long routeId = reset.routeId();
        final long streamId = reset.streamId();
        final long traceId = reset.traceId();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(throttleFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                "RESET");
    }

    private void onWindow(
        final WindowFW window)
    {
        final int offset = window.offset() - HEADER_LENGTH;
        final long timestamp = window.timestamp();
        final long routeId = window.routeId();
        final long streamId = window.streamId();
        final long traceId = window.traceId();
        final int credit = window.credit();
        final int padding = window.padding();
        final long budgetId = window.budgetId();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(throttleFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                format("WINDOW [0x%016x] [%d] [%d]", budgetId, credit, padding));
    }

    private void onSignal(
        final SignalFW signal)
    {
        final int offset = signal.offset() - HEADER_LENGTH;
        final long timestamp = signal.timestamp();
        final long routeId = signal.routeId();
        final long streamId = signal.streamId();
        final long traceId = signal.traceId();
        final long authorization = signal.authorization();
        final long signalId = signal.signalId();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(throttleFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                format("SIGNAL [%d] [0x%016x]", signalId, authorization));
    }

    private void onChallenge(
        final ChallengeFW challenge)
    {
        final int offset = challenge.offset() - HEADER_LENGTH;
        final long timestamp = challenge.timestamp();
        final long routeId = challenge.routeId();
        final long streamId = challenge.streamId();
        final long traceId = challenge.traceId();
        final long authorization = challenge.authorization();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(throttleFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                format("CHALLENGE [0x%016x]", authorization));
    }

    private void onFlush(
        FlushFW flush)
    {
        final int offset = flush.offset() - HEADER_LENGTH;
        final long timestamp = flush.timestamp();
        final long routeId = flush.routeId();
        final long streamId = flush.streamId();
        final long traceId = flush.traceId();
        final long authorization = flush.authorization();
        final long budgetId = flush.budgetId();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(throttleFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                format("FLUSH [0x%016x] [0x%016x]", budgetId, authorization));
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
                    b.getBytes(o, address);
                    return address;
                });
                socketAddress = new InetSocketAddress(getByAddress(address), tcpPort);
                break;
            case TcpAddressFW.KIND_IPV6_ADDRESS:
                address = new byte[16];
                tcpAddress.ipv4Address().get((b, o, l) ->
                {
                    b.getBytes(o, address);
                    return address;
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

    private void onTcpBeginEx(
        final BeginFW begin)
    {
        final int offset = begin.offset() - HEADER_LENGTH;
        final long timestamp = begin.timestamp();
        final OctetsFW extension = begin.extension();

        final TcpBeginExFW tcpBeginEx = tcpBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        final InetSocketAddress localAddress = toInetSocketAddress(tcpBeginEx.localAddress(), tcpBeginEx.localPort());
        final InetSocketAddress remoteAddress = toInetSocketAddress(tcpBeginEx.remoteAddress(), tcpBeginEx.remotePort());

        out.printf(verboseFormat, index, offset, timestamp, format("%s\t%s", localAddress, remoteAddress));
    }

    private void onTlsBeginEx(
        final BeginFW begin)
    {
        final int offset = begin.offset() - HEADER_LENGTH;
        final long timestamp = begin.timestamp();
        final OctetsFW extension = begin.extension();

        final TlsBeginExFW tlsBeginEx = tlsBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        final String hostname = tlsBeginEx.hostname().asString();
        final String protocol = tlsBeginEx.protocol().asString();

        out.printf(verboseFormat, index, offset, timestamp, format("%s\t%s", hostname, protocol));
    }

    private void onHttpBeginEx(
        final BeginFW begin)
    {
        final int offset = begin.offset() - HEADER_LENGTH;
        final long timestamp = begin.timestamp();
        final OctetsFW extension = begin.extension();

        final HttpBeginExFW httpBeginEx = httpBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        httpBeginEx.headers()
                   .forEach(h -> out.printf(verboseFormat, index, offset, timestamp,
                                           format("%s: %s", h.name().asString(), h.value().asString())));
    }

    private void onHttpDataEx(
        final DataFW data)
    {
        final int offset = data.offset() - HEADER_LENGTH;
        final long timestamp = data.timestamp();
        final OctetsFW extension = data.extension();

        final HttpDataExFW httpDataEx = httpDataExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        httpDataEx.promise()
                   .forEach(h -> out.printf(verboseFormat, index, offset, timestamp,
                       format("%s: %s", h.name().asString(), h.value().asString())));
    }

    private void onHttpEndEx(
        final EndFW end)
    {
        final int offset = end.offset() - HEADER_LENGTH;
        final long timestamp = end.timestamp();
        final OctetsFW extension = end.extension();

        final HttpEndExFW httpEndEx = httpEndExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        httpEndEx.trailers()
                 .forEach(h -> out.printf(verboseFormat, index, offset, timestamp,
                                         format("%s: %s", h.name().asString(), h.value().asString())));
    }

    private void onKafkaBeginEx(
        final BeginFW begin)
    {
        final int offset = begin.offset() - HEADER_LENGTH;
        final long timestamp = begin.timestamp();
        final OctetsFW extension = begin.extension();

        final KafkaBeginExFW kafkaBeginEx = kafkaBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        switch (kafkaBeginEx.kind())
        {
        case KafkaBeginExFW.KIND_DESCRIBE:
            onKafkaDescribeBeginEx(offset, timestamp, kafkaBeginEx.describe());
            break;
        case KafkaBeginExFW.KIND_FETCH:
            onKafkaFetchBeginEx(offset, timestamp, kafkaBeginEx.fetch());
            break;
        case KafkaBeginExFW.KIND_MERGED:
            onKafkaMergedBeginEx(offset, timestamp, kafkaBeginEx.merged());
            break;
        case KafkaBeginExFW.KIND_META:
            onKafkaMetaBeginEx(offset, timestamp, kafkaBeginEx.meta());
            break;
        case KafkaBeginExFW.KIND_PRODUCE:
            onKafkaProduceBeginEx(offset, timestamp, kafkaBeginEx.produce());
            break;
        }
    }

    private void onKafkaDescribeBeginEx(
        int offset,
        long timestamp,
        KafkaDescribeBeginExFW describe)
    {
        final ArrayFW<String16FW> configs = describe.configs();

        out.printf(verboseFormat, index, offset, timestamp, "[describe]");
        configs.forEach(c -> out.printf(verboseFormat, index, offset, timestamp, c.asString()));
    }

    private void onKafkaFetchBeginEx(
        int offset,
        long timestamp,
        KafkaFetchBeginExFW fetch)
    {
        final String16FW topic = fetch.topic();
        final KafkaOffsetFW partition = fetch.partition();

        out.printf(verboseFormat, index, offset, timestamp, format("[fetch] %s", topic.asString()));
        out.printf(verboseFormat, index, offset, timestamp, format("%d: %d", partition.partitionId(), partition.offset$()));
    }

    private void onKafkaMergedBeginEx(
        int offset,
        long timestamp,
        KafkaMergedBeginExFW merged)
    {
        final String16FW topic = merged.topic();
        final ArrayFW<KafkaOffsetFW> partitions = merged.partitions();

        out.printf(verboseFormat, index, offset, timestamp, format("[merged] %s", topic.asString()));
        partitions.forEach(p -> out.printf(verboseFormat, index, offset, timestamp,
                                         format("%d: %d", p.partitionId(), p.offset$())));
    }

    private void onKafkaMetaBeginEx(
        int offset,
        long timestamp,
        KafkaMetaBeginExFW meta)
    {
        final String16FW topic = meta.topic();

        out.printf(verboseFormat, index, offset, timestamp, format("[meta] %s", topic.asString()));
    }

    private void onKafkaProduceBeginEx(
        int offset,
        long timestamp,
        KafkaProduceBeginExFW produce)
    {
        final String16FW topic = produce.topic();
        final long producerId = produce.producerId();
        final StringFW transaction = produce.transaction();

        out.printf(verboseFormat, index, offset, timestamp,
                   format("[produce] %s %d %s", topic.asString(), producerId, transaction.asString()));
    }

    private void onKafkaDataEx(
        final DataFW data)
    {
        final int offset = data.offset() - HEADER_LENGTH;
        final long timestamp = data.timestamp();
        final OctetsFW extension = data.extension();

        final KafkaDataExFW kafkaDataEx = kafkaDataExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        switch (kafkaDataEx.kind())
        {
        case KafkaDataExFW.KIND_DESCRIBE:
            onKafkaDescribeDataEx(offset, timestamp, kafkaDataEx.describe());
            break;
        case KafkaDataExFW.KIND_FETCH:
            onKafkaFetchDataEx(offset, timestamp, kafkaDataEx.fetch());
            break;
        case KafkaDataExFW.KIND_MERGED:
            onKafkaMergedDataEx(offset, timestamp, kafkaDataEx.merged());
            break;
        case KafkaDataExFW.KIND_META:
            onKafkaMetaDataEx(offset, timestamp, kafkaDataEx.meta());
            break;
        case KafkaDataExFW.KIND_PRODUCE:
            onKafkaProduceDataEx(offset, timestamp, kafkaDataEx.produce());
            break;
        }
    }

    private void onKafkaDescribeDataEx(
        int offset,
        long timestamp,
        KafkaDescribeDataExFW describe)
    {
        final ArrayFW<KafkaConfigFW> configs = describe.configs();

        out.printf(verboseFormat, index, offset, timestamp, "[describe]");
        configs.forEach(c -> out.printf(verboseFormat, index, offset, timestamp,
                                        format("%s: %s", c.name().asString(), c.value().asString())));
    }

    private void onKafkaFetchDataEx(
        int offset,
        long timestamp,
        KafkaFetchDataExFW fetch)
    {
        final KafkaKeyFW key = fetch.key();
        final ArrayFW<KafkaHeaderFW> headers = fetch.headers();
        final KafkaOffsetFW partition = fetch.partition();

        out.printf(verboseFormat, index, offset, timestamp,
                   format("[fetch] %d %s %d %d",
                           fetch.timestamp(), asString(key.value()),
                           partition.partitionId(), partition.offset$()));
        headers.forEach(h -> out.printf(verboseFormat, index, offset, timestamp,
                                        format("%s: %s", asString(h.name()), asString(h.value()))));
    }

    private void onKafkaMergedDataEx(
        int offset,
        long timestamp,
        KafkaMergedDataExFW merged)
    {
        final KafkaKeyFW key = merged.key();
        final ArrayFW<KafkaHeaderFW> headers = merged.headers();
        final KafkaOffsetFW partition = merged.partition();
        final ArrayFW<KafkaOffsetFW> progress = merged.progress();

        out.printf(verboseFormat, index, offset, timestamp,
                   format("[merged] %d %s %d %d",
                           merged.timestamp(), asString(key.value()),
                           partition.partitionId(), partition.offset$()));
        headers.forEach(h -> out.printf(verboseFormat, index, offset, timestamp,
                                        format("%s: %s", asString(h.name()), asString(h.value()))));
        progress.forEach(p -> out.printf(verboseFormat, index, offset, timestamp,
                                         format("%d: %d", p.partitionId(), p.offset$())));
    }

    private void onKafkaMetaDataEx(
        int offset,
        long timestamp,
        KafkaMetaDataExFW meta)
    {
        final ArrayFW<KafkaPartitionFW> partitions = meta.partitions();

        out.printf(verboseFormat, index, offset, timestamp, "[meta]");
        partitions.forEach(p -> out.printf(verboseFormat, index, offset, timestamp,
                                           format("%d: %d", p.partitionId(), p.leaderId())));
    }

    private void onKafkaProduceDataEx(
        int offset,
        long timestamp,
        KafkaProduceDataExFW produce)
    {
        final KafkaKeyFW key = produce.key();
        final ArrayFW<KafkaHeaderFW> headers = produce.headers();
        final KafkaOffsetFW progress = produce.progress();

        out.printf(verboseFormat, index, offset, timestamp,
                   format("[produce] %s", asString(key.value())));
        headers.forEach(h -> out.printf(verboseFormat, index, offset, timestamp,
                                        format("%s: %s", asString(h.name()), asString(h.value()))));
        out.printf(verboseFormat, index, offset, timestamp,
                   format("%d: %d", progress.partitionId(), progress.offset$()));
    }

    private static String asString(
        OctetsFW value)
    {
        return value != null
            ? value.buffer().getStringWithoutLengthUtf8(value.offset(), value.sizeof())
            : "null";
    }
}
