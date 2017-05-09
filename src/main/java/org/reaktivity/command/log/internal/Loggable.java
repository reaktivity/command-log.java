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
import org.reaktivity.command.log.internal.types.stream.BeginFW;
import org.reaktivity.command.log.internal.types.stream.DataFW;
import org.reaktivity.command.log.internal.types.stream.EndFW;
import org.reaktivity.command.log.internal.types.stream.ResetFW;
import org.reaktivity.command.log.internal.types.stream.WindowFW;

public final class Loggable implements AutoCloseable
{

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();

    private final String streamFormat;
    private final String throttleFormat;
    private final StreamsLayout layout;
    private final RingBufferSpy streamsBuffer;
    private final RingBufferSpy throttleBuffer;

    public Loggable(
        String receiver,
        String sender,
        StreamsLayout layout)
    {
        this.streamFormat = String.format("[%s -> %s]\t[0x%%016x] %%s", sender, receiver);
        this.throttleFormat = String.format("[%s <- %s]\t[0x%%016x] %%s", sender, receiver);
        this.layout = layout;
        this.streamsBuffer = layout.streamsBuffer();
        this.throttleBuffer = layout.throttleBuffer();
    }

    public int process()
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
            System.out.println(format(streamFormat, begin.streamId(),
                    format("BEGIN [0x%016x] [0x%016x]", begin.referenceId(), begin.correlationId())));
            break;
        case DataFW.TYPE_ID:
            final DataFW data = dataRO.wrap(buffer, index, index + length);
            System.out.println(format(streamFormat, data.streamId(), format("DATA [%d]", data.length())));
            break;
        case EndFW.TYPE_ID:
            final EndFW end = endRO.wrap(buffer, index, index + length);
            System.out.println(format(streamFormat, end.streamId(), "END"));
            break;
        }
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
            System.out.println(format(throttleFormat, reset.streamId(), "RESET"));
            break;
        case WindowFW.TYPE_ID:
            final WindowFW window = windowRO.wrap(buffer, index, index + length);
            System.out.println(format(throttleFormat, window.streamId(), format("WINDOW [%d]", window.update())));
            break;
        }
    }
}
