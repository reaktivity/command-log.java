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
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.reaktivity.command.log.internal.labels.LabelManager;
import org.reaktivity.command.log.internal.layouts.StreamsLayout;
import org.reaktivity.command.log.internal.spy.RingBufferSpy;
import org.reaktivity.command.log.internal.types.AmqpPropertiesFW;
import org.reaktivity.command.log.internal.types.Array32FW;
import org.reaktivity.command.log.internal.types.ArrayFW;
import org.reaktivity.command.log.internal.types.KafkaAgeFW;
import org.reaktivity.command.log.internal.types.KafkaCapabilities;
import org.reaktivity.command.log.internal.types.KafkaConditionFW;
import org.reaktivity.command.log.internal.types.KafkaConfigFW;
import org.reaktivity.command.log.internal.types.KafkaFilterFW;
import org.reaktivity.command.log.internal.types.KafkaHeaderFW;
import org.reaktivity.command.log.internal.types.KafkaKeyFW;
import org.reaktivity.command.log.internal.types.KafkaOffsetFW;
import org.reaktivity.command.log.internal.types.KafkaPartitionFW;
import org.reaktivity.command.log.internal.types.MqttCapabilities;
import org.reaktivity.command.log.internal.types.MqttCapabilitiesFW;
import org.reaktivity.command.log.internal.types.MqttUserPropertyFW;
import org.reaktivity.command.log.internal.types.OctetsFW;
import org.reaktivity.command.log.internal.types.String16FW;
import org.reaktivity.command.log.internal.types.StringFW;
import org.reaktivity.command.log.internal.types.TcpAddressFW;
import org.reaktivity.command.log.internal.types.stream.AbortFW;
import org.reaktivity.command.log.internal.types.stream.AmqpBeginExFW;
import org.reaktivity.command.log.internal.types.stream.AmqpDataExFW;
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
import org.reaktivity.command.log.internal.types.stream.KafkaBootstrapBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaDescribeBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaDescribeDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaFetchBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaFetchDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaFetchFlushExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaFlushExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMergedBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMergedDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMergedFlushExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMetaBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaMetaDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaProduceBeginExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaProduceDataExFW;
import org.reaktivity.command.log.internal.types.stream.KafkaResetExFW;
import org.reaktivity.command.log.internal.types.stream.MqttBeginExFW;
import org.reaktivity.command.log.internal.types.stream.MqttDataExFW;
import org.reaktivity.command.log.internal.types.stream.MqttFlushExFW;
import org.reaktivity.command.log.internal.types.stream.ResetFW;
import org.reaktivity.command.log.internal.types.stream.SignalFW;
import org.reaktivity.command.log.internal.types.stream.TcpBeginExFW;
import org.reaktivity.command.log.internal.types.stream.TlsBeginExFW;
import org.reaktivity.command.log.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.function.MessageConsumer;

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
    private final KafkaFlushExFW kafkaFlushExRO = new KafkaFlushExFW();
    private final KafkaResetExFW kafkaResetExRO = new KafkaResetExFW();
    private final MqttBeginExFW mqttBeginExRO = new MqttBeginExFW();
    private final MqttDataExFW mqttDataExRO = new MqttDataExFW();
    private final MqttFlushExFW mqttFlushExRO = new MqttFlushExFW();
    private final AmqpBeginExFW amqpBeginExRO = new AmqpBeginExFW();
    private final AmqpDataExFW amqpDataExRO = new AmqpDataExFW();

    private final int index;
    private final LabelManager labels;
    private final String streamFormat;
    private final String throttleFormat;
    private final String verboseFormat;
    private final StreamsLayout layout;
    private final RingBufferSpy streamsBuffer;
    private final Logger out;
    private final LongPredicate nextTimestamp;

    private final Int2ObjectHashMap<MessageConsumer> frameHandlers;
    private final Int2ObjectHashMap<Consumer<BeginFW>> beginHandlers;
    private final Int2ObjectHashMap<Consumer<DataFW>> dataHandlers;
    private final Int2ObjectHashMap<Consumer<EndFW>> endHandlers;
    private final Int2ObjectHashMap<Consumer<FlushFW>> flushHandlers;
    private final Int2ObjectHashMap<Consumer<ResetFW>> resetHandlers;

    LoggableStream(
        int index,
        LabelManager labels,
        StreamsLayout layout,
        Logger logger,
        Predicate<String> hasFrameType,
        Predicate<String> hasExtensionType,
        LongPredicate nextTimestamp)
    {
        this.index = index;
        this.labels = labels;
        this.streamFormat = "[%02d/%08x] [%d] [0x%016x] [%s -> %s]\t[0x%016x] [0x%016x] %s\n";
        this.throttleFormat = "[%02d/%08x] [%d] [0x%016x] [%s <- %s]\t[0x%016x] [0x%016x] %s\n";
        this.verboseFormat = "[%02d/%08x] [%d] %s\n";

        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();
        this.out = logger;
        this.nextTimestamp = nextTimestamp;

        final Int2ObjectHashMap<MessageConsumer> frameHandlers = new Int2ObjectHashMap<>();
        final Int2ObjectHashMap<Consumer<BeginFW>> beginHandlers = new Int2ObjectHashMap<>();
        final Int2ObjectHashMap<Consumer<DataFW>> dataHandlers = new Int2ObjectHashMap<>();
        final Int2ObjectHashMap<Consumer<EndFW>> endHandlers = new Int2ObjectHashMap<>();
        final Int2ObjectHashMap<Consumer<FlushFW>> flushHandlers = new Int2ObjectHashMap<>();
        final Int2ObjectHashMap<Consumer<ResetFW>> resetHandlers = new Int2ObjectHashMap<>();

        if (hasFrameType.test("BEGIN"))
        {
            frameHandlers.put(BeginFW.TYPE_ID, (t, b, i, l) -> onBegin(beginRO.wrap(b, i, i + l)));
        }
        if (hasFrameType.test("DATA"))
        {
            frameHandlers.put(DataFW.TYPE_ID, (t, b, i, l) -> onData(dataRO.wrap(b, i, i + l)));
        }
        if (hasFrameType.test("END"))
        {
            frameHandlers.put(EndFW.TYPE_ID, (t, b, i, l) -> onEnd(endRO.wrap(b, i, i + l)));
        }
        if (hasFrameType.test("ABORT"))
        {
            frameHandlers.put(AbortFW.TYPE_ID, (t, b, i, l) -> onAbort(abortRO.wrap(b, i, i + l)));
        }
        if (hasFrameType.test("WINDOW"))
        {
            frameHandlers.put(WindowFW.TYPE_ID, (t, b, i, l) -> onWindow(windowRO.wrap(b, i, i + l)));
        }
        if (hasFrameType.test("RESET"))
        {
            frameHandlers.put(ResetFW.TYPE_ID, (t, b, i, l) -> onReset(resetRO.wrap(b, i, i + l)));
        }
        if (hasFrameType.test("CHALLENGE"))
        {
            frameHandlers.put(ChallengeFW.TYPE_ID, (t, b, i, l) -> onChallenge(challengeRO.wrap(b, i, i + l)));
        }
        if (hasFrameType.test("SIGNAL"))
        {
            frameHandlers.put(SignalFW.TYPE_ID, (t, b, i, l) -> onSignal(signalRO.wrap(b, i, i + l)));
        }
        if (hasFrameType.test("FLUSH"))
        {
            frameHandlers.put(FlushFW.TYPE_ID, (t, b, i, l) -> onFlush(flushRO.wrap(b, i, i + l)));
        }

        if (hasExtensionType.test("tcp"))
        {
            beginHandlers.put(labels.lookupLabelId("tcp"), this::onTcpBeginEx);
        }

        if (hasExtensionType.test("tls"))
        {
            beginHandlers.put(labels.lookupLabelId("tls"), this::onTlsBeginEx);
        }

        if (hasExtensionType.test("http"))
        {
            beginHandlers.put(labels.lookupLabelId("http"), this::onHttpBeginEx);
            dataHandlers.put(labels.lookupLabelId("http"), this::onHttpDataEx);
            endHandlers.put(labels.lookupLabelId("http"), this::onHttpEndEx);
        }

        if (hasExtensionType.test("kafka"))
        {
            beginHandlers.put(labels.lookupLabelId("kafka"), this::onKafkaBeginEx);
            dataHandlers.put(labels.lookupLabelId("kafka"), this::onKafkaDataEx);
            flushHandlers.put(labels.lookupLabelId("kafka"), this::onKafkaFlushEx);
            resetHandlers.put(labels.lookupLabelId("kafka"), this::onKafkaResetEx);
        }

        if (hasExtensionType.test("mqtt"))
        {
            beginHandlers.put(labels.lookupLabelId("mqtt"), this::onMqttBeginEx);
            dataHandlers.put(labels.lookupLabelId("mqtt"), this::onMqttDataEx);
            flushHandlers.put(labels.lookupLabelId("mqtt"), this::onMqttFlushEx);
        }

        if (hasExtensionType.test("amqp"))
        {
            beginHandlers.put(labels.lookupLabelId("amqp"), this::onAmqpBeginEx);
            dataHandlers.put(labels.lookupLabelId("amqp"), this::onAmqpDataEx);
        }

        this.frameHandlers = frameHandlers;
        this.beginHandlers = beginHandlers;
        this.dataHandlers = dataHandlers;
        this.endHandlers = endHandlers;
        this.flushHandlers = flushHandlers;
        this.resetHandlers = resetHandlers;
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

        final MessageConsumer handler = frameHandlers.get(msgTypeId);
        if (handler != null)
        {
            handler.accept(msgTypeId, buffer, index, length);
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

        final ExtensionFW extension = reset.extension().get(extensionRO::tryWrap);
        if (extension != null)
        {
            final Consumer<ResetFW> resetHandler = resetHandlers.get(extension.typeId());
            if (resetHandler != null)
            {
                resetHandler.accept(reset);
            }
        }
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
        final int minimum = window.minimum();
        final long initialId = streamId | 0x0000_0000_0000_0001L;

        final int localId = (int)(routeId >> 48) & 0xffff;
        final int remoteId = (int)(routeId >> 32) & 0xffff;
        final int sourceId = streamId == initialId ? localId : remoteId;
        final int targetId = streamId == initialId ? remoteId : localId;
        final String sourceName = labels.lookupLabel(sourceId);
        final String targetName = labels.lookupLabel(targetId);

        out.printf(throttleFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                format("WINDOW [0x%016x] [%d] [%d] [%d]", budgetId, credit, padding, minimum));
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
        final FlushFW flush)
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

        out.printf(streamFormat, index, offset, timestamp, traceId, sourceName, targetName, routeId, streamId,
                format("FLUSH [0x%016x] [0x%016x]", budgetId, authorization));

        final ExtensionFW extension = flush.extension().get(extensionRO::tryWrap);
        if (extension != null)
        {
            final Consumer<FlushFW> flushHandler = flushHandlers.get(extension.typeId());
            if (flushHandler != null)
            {
                flushHandler.accept(flush);
            }
        }
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
        case KafkaBeginExFW.KIND_BOOTSTRAP:
            onKafkaBootstrapBeginEx(offset, timestamp, kafkaBeginEx.bootstrap());
            break;
        case KafkaBeginExFW.KIND_MERGED:
            onKafkaMergedBeginEx(offset, timestamp, kafkaBeginEx.merged());
            break;
        case KafkaBeginExFW.KIND_DESCRIBE:
            onKafkaDescribeBeginEx(offset, timestamp, kafkaBeginEx.describe());
            break;
        case KafkaBeginExFW.KIND_FETCH:
            onKafkaFetchBeginEx(offset, timestamp, kafkaBeginEx.fetch());
            break;
        case KafkaBeginExFW.KIND_META:
            onKafkaMetaBeginEx(offset, timestamp, kafkaBeginEx.meta());
            break;
        case KafkaBeginExFW.KIND_PRODUCE:
            onKafkaProduceBeginEx(offset, timestamp, kafkaBeginEx.produce());
            break;
        }
    }

    private void onKafkaBootstrapBeginEx(
        int offset,
        long timestamp,
        KafkaBootstrapBeginExFW bootstrap)
    {
        final String16FW topic = bootstrap.topic();

        out.printf(verboseFormat, index, offset, timestamp, format("[bootstrap] %s", topic.asString()));
    }

    private void onKafkaMergedBeginEx(
        int offset,
        long timestamp,
        KafkaMergedBeginExFW merged)
    {
        final String16FW topic = merged.topic();
        final ArrayFW<KafkaOffsetFW> partitions = merged.partitions();
        final KafkaCapabilities capabilities = merged.capabilities().get();

        out.printf(verboseFormat, index, offset, timestamp, format("[merged] %s %s", topic.asString(), capabilities));
        partitions.forEach(p -> out.printf(verboseFormat, index, offset, timestamp,
                                         format("%d: %d", p.partitionId(), p.partitionOffset())));
    }

    private void onKafkaDescribeBeginEx(
        int offset,
        long timestamp,
        KafkaDescribeBeginExFW describe)
    {
        final String16FW topic = describe.topic();
        final ArrayFW<String16FW> configs = describe.configs();

        out.printf(verboseFormat, index, offset, timestamp, format("[describe] %s", topic.asString()));
        configs.forEach(c -> out.printf(verboseFormat, index, offset, timestamp, c.asString()));
    }

    private void onKafkaFetchBeginEx(
        int offset,
        long timestamp,
        KafkaFetchBeginExFW fetch)
    {
        final String16FW topic = fetch.topic();
        final KafkaOffsetFW partition = fetch.partition();
        final ArrayFW<KafkaFilterFW> filters = fetch.filters();

        out.printf(verboseFormat, index, offset, timestamp, format("[fetch] %s", topic.asString()));
        out.printf(verboseFormat, index, offset, timestamp,
                   format("%d: %d", partition.partitionId(), partition.partitionOffset()));
        filters.forEach(f -> f.conditions().forEach(c -> out.printf(verboseFormat, index, offset, timestamp, asString(c))));
    }

    private String asString(
        KafkaConditionFW condition)
    {
        String formatted = "unknown";
        switch (condition.kind())
        {
        case KafkaConditionFW.KIND_KEY:
            final KafkaKeyFW key = condition.key();
            formatted = String.format("key[%d]", key.length());
            break;
        case KafkaConditionFW.KIND_HEADER:
            final KafkaHeaderFW header = condition.header();
            final OctetsFW name = header.name();
            final String formattedName = name.buffer().getStringWithoutLengthUtf8(name.offset(), name.sizeof());
            formatted = String.format("header[%s=[%d]]", formattedName, header.valueLen());
            break;
        case KafkaConditionFW.KIND_AGE:
            final KafkaAgeFW age = condition.age();
            formatted = String.format("age[%s]", age.get());
            break;
        }
        return formatted;
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
        final long partitionId = produce.partitionId();
        final StringFW transaction = produce.transaction();

        out.printf(verboseFormat, index, offset, timestamp,
                   format("[produce] %s %d %s", topic.asString(), partitionId, transaction.asString()));
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
                   format("[fetch] (%d) %d %s %d %d",
                           fetch.deferred(), fetch.timestamp(), asString(key.value()),
                           partition.partitionId(), partition.partitionOffset()));
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
                   format("[merged] (%d) %d %s %d %d",
                           merged.deferred(), merged.timestamp(), asString(key.value()),
                           partition.partitionId(), partition.partitionOffset()));
        headers.forEach(h -> out.printf(verboseFormat, index, offset, timestamp,
                                        format("%s: %s", asString(h.name()), asString(h.value()))));
        progress.forEach(p -> out.printf(verboseFormat, index, offset, timestamp,
                                         format("%d: %d", p.partitionId(), p.partitionOffset())));
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

        out.printf(verboseFormat, index, offset, timestamp,
                   format("[produce] (%d) %s", produce.deferred(), asString(key.value())));
        headers.forEach(h -> out.printf(verboseFormat, index, offset, timestamp,
                                        format("%s: %s", asString(h.name()), asString(h.value()))));
    }

    private void onKafkaFlushEx(
        final FlushFW flush)
    {
        final int offset = flush.offset() - HEADER_LENGTH;
        final long timestamp = flush.timestamp();
        final OctetsFW extension = flush.extension();

        final KafkaFlushExFW kafkaFlushEx = kafkaFlushExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        switch (kafkaFlushEx.kind())
        {
        case KafkaFlushExFW.KIND_MERGED:
            onKafkaMergedFlushEx(offset, timestamp, kafkaFlushEx.merged());
            break;
        case KafkaFlushExFW.KIND_FETCH:
            onKafkaFetchFlushEx(offset, timestamp, kafkaFlushEx.fetch());
            break;
        }
    }

    private void onKafkaMergedFlushEx(
        int offset,
        long timestamp,
        KafkaMergedFlushExFW merged)
    {
        final ArrayFW<KafkaOffsetFW> progress = merged.progress();

        out.printf(verboseFormat, index, offset, timestamp, "[merged]");
        progress.forEach(p -> out.printf(verboseFormat, index, offset, timestamp,
                   format("%d: %d", p.partitionId(), p.partitionOffset())));
    }

    private void onKafkaFetchFlushEx(
        int offset,
        long timestamp,
        KafkaFetchFlushExFW fetch)
    {
        final KafkaOffsetFW partition = fetch.partition();

        out.printf(verboseFormat, index, offset, timestamp,
                format("[fetch] %d %d", partition.partitionId(), partition.partitionOffset()));
    }

    private void onKafkaResetEx(
        final ResetFW reset)
    {
        final int offset = reset.offset() - HEADER_LENGTH;
        final long timestamp = reset.timestamp();
        final OctetsFW extension = reset.extension();

        final KafkaResetExFW kafkaResetEx = kafkaResetExRO.wrap(extension.buffer(), extension.offset(), extension.limit());

        final int error = kafkaResetEx.error();

        out.printf(verboseFormat, index, offset, timestamp, format("error %d", error));
    }

    private void onMqttBeginEx(
        final BeginFW begin)
    {
        final int offset = begin.offset() - HEADER_LENGTH;
        final long timestamp = begin.timestamp();
        final OctetsFW extension = begin.extension();

        final MqttBeginExFW mqttBeginEx = mqttBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        final MqttCapabilities capabilities = mqttBeginEx.capabilities().get();
        final String clientId = mqttBeginEx.clientId().asString();
        final String topic = mqttBeginEx.topic().asString();
        final int subscriptionId = mqttBeginEx.subscriptionId();
        final Array32FW<MqttUserPropertyFW> properties = mqttBeginEx.properties();

        out.printf(verboseFormat, index, offset, timestamp, format("capabilities: %s", capabilities));
        out.printf(verboseFormat, index, offset, timestamp, format("clientId: %s", clientId));
        out.printf(verboseFormat, index, offset, timestamp, format("topic: %s", topic));
        out.printf(verboseFormat, index, offset, timestamp, format("subscriptionId: %s", subscriptionId));
        properties.forEach(p -> out.printf(verboseFormat, index, offset, timestamp,
                format("%d: %d", p.key().asString(), p.value().asString())));
    }

    private void onMqttDataEx(
        final DataFW data)
    {
        final int offset = data.offset() - HEADER_LENGTH;
        final long timestamp = data.timestamp();
        final OctetsFW extension = data.extension();

        final MqttDataExFW mqttDataEx = mqttDataExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        final String contentType = mqttDataEx.contentType().asString();
        final int correlationBytes = mqttDataEx.correlation().length();
        final int deferred = mqttDataEx.deferred();
        final int expiryInterval = mqttDataEx.expiryInterval();
        final String responseTopic = mqttDataEx.responseTopic().asString();
        final String topic = mqttDataEx.topic().asString();
        final Array32FW<MqttUserPropertyFW> properties = mqttDataEx.properties();

        out.printf(verboseFormat, index, offset, timestamp, format("contentType: %s", contentType));
        out.printf(verboseFormat, index, offset, timestamp, format("responseTopic: %s", responseTopic));
        out.printf(verboseFormat, index, offset, timestamp, format("topic: %s", topic));
        out.printf(verboseFormat, index, offset, timestamp, format("correlationBytes: %d bytes", correlationBytes));
        out.printf(verboseFormat, index, offset, timestamp, format("deferred: %d", deferred));
        out.printf(verboseFormat, index, offset, timestamp, format("expiryInterval: %d", expiryInterval));
        properties.forEach(p -> out.printf(verboseFormat, index, offset, timestamp,
                format("%s: %s", p.key().asString(), p.value().asString())));
    }

    private void onMqttFlushEx(
        final FlushFW flush)
    {
        final int offset = flush.offset() - HEADER_LENGTH;
        final long timestamp = flush.timestamp();
        final OctetsFW extension = flush.extension();

        final MqttFlushExFW mqttFlushEx = mqttFlushExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        final MqttCapabilitiesFW capabilities = mqttFlushEx.capabilities();

        out.printf(verboseFormat, index, offset, timestamp, format("capabilities: %s", capabilities));
    }

    private void onAmqpBeginEx(
        final BeginFW begin)
    {
        final int offset = begin.offset() - HEADER_LENGTH;
        final long timestamp = begin.timestamp();
        final OctetsFW extension = begin.extension();

        final AmqpBeginExFW amqpBeginEx = amqpBeginExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        final String address = amqpBeginEx.address().asString();
        final String capabilities = amqpBeginEx.capabilities().toString();
        final String senderSettleMode = amqpBeginEx.senderSettleMode().toString();
        final String receiverSettleMode = amqpBeginEx.receiverSettleMode().toString();

        out.printf(verboseFormat, index, offset, timestamp, format("address: %s", address));
        out.printf(verboseFormat, index, offset, timestamp, format("capabilities: %s", capabilities));
        out.printf(verboseFormat, index, offset, timestamp, format("senderSettleMode: %s", senderSettleMode));
        out.printf(verboseFormat, index, offset, timestamp, format("receiverSettleMode: %s", receiverSettleMode));
    }

    private void onAmqpDataEx(
        final DataFW data)
    {
        final int offset = data.offset() - HEADER_LENGTH;
        final long timestamp = data.timestamp();
        final OctetsFW extension = data.extension();

        final AmqpDataExFW amqpDataEx = amqpDataExRO.wrap(extension.buffer(), extension.offset(), extension.limit());
        final long deliveryId = amqpDataEx.deliveryId();
        final long messageFormat = amqpDataEx.messageFormat();
        final int flags = amqpDataEx.flags();
        final AmqpPropertiesFW properties = amqpDataEx.properties();

        out.printf(verboseFormat, index, offset, timestamp, format("deliveryId: %d", deliveryId));
        out.printf(verboseFormat, index, offset, timestamp, format("deliveryTag: %s", amqpDataEx.deliveryTag()));
        out.printf(verboseFormat, index, offset, timestamp, format("messageFormat: %d", messageFormat));
        out.printf(verboseFormat, index, offset, timestamp, format("flags: %d", flags));
        amqpDataEx.annotations().forEach(a -> out.printf(verboseFormat, index, offset, timestamp,
            format("annotation: [key:%s] [value:%s]", a.key(), a.value())));
        if (properties.hasMessageId())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("messageId: %s", properties.messageId()));
        }
        if (properties.hasUserId())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("userId: %s", properties.userId()));
        }
        if (properties.hasTo())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("to: %s", properties.to().asString()));
        }
        if (properties.hasSubject())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("subject: %s", properties.subject().asString()));
        }
        if (properties.hasReplyTo())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("replyTo: %s", properties.replyTo().asString()));
        }
        if (properties.hasCorrelationId())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("correlationId: %s",
                properties.correlationId()));
        }
        if (properties.hasContentType())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("contentType: %s",
                properties.contentType().asString()));
        }
        if (properties.hasContentEncoding())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("contentEncoding: %s",
                properties.contentEncoding().asString()));
        }
        if (properties.hasAbsoluteExpiryTime())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("absoluteExpiryTime: %d",
                properties.absoluteExpiryTime()));
        }
        if (properties.hasCreationTime())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("creationTime: %d", properties.creationTime()));
        }
        if (properties.hasGroupId())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("groupId: %s", properties.groupId().asString()));
        }
        if (properties.hasGroupSequence())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("groupSequence: %d", properties.groupSequence()));
        }
        if (properties.hasReplyToGroupId())
        {
            out.printf(verboseFormat, index, offset, timestamp, format("replyToGroupId: %s",
                properties.replyToGroupId().asString()));
        }
        amqpDataEx.applicationProperties().forEach(a -> out.printf(verboseFormat, index, offset, timestamp,
            format("applicationProperty: [key:%s] [value:%s]", a.key(), a.value())));
    }

    private static String asString(
        OctetsFW value)
    {
        return value != null
            ? value.buffer().getStringWithoutLengthUtf8(value.offset(), value.sizeof())
            : "null";
    }
}
