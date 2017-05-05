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

import static org.agrona.BitUtil.SIZE_OF_LONG;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.command.log.internal.layouts.StreamsLayout;
import org.reaktivity.command.log.internal.spy.RingBufferSpy;

public final class Loggable implements AutoCloseable
{
    private final String receiver;
    private final String sender;
    private final StreamsLayout layout;
    private final RingBufferSpy streamsBuffer;
    private final RingBufferSpy throttleBuffer;

    public Loggable(
        String receiver,
        String sender,
        StreamsLayout layout)
    {
        this.receiver = receiver;
        this.sender = sender;
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
        final long streamId = buffer.getLong(index);

        switch (msgTypeId)
        {
        case 0x00000001:
            System.out.println(String.format("[%s -> %s]\t[0x%016x] BEGIN", sender, receiver, streamId));
            break;
        case 0x00000002:
            final int payloadBytes = buffer.getShort(index + SIZE_OF_LONG);
            System.out.println(String.format("[%s -> %s]\t[0x%016x] DATA (%d)", sender, receiver, streamId, payloadBytes));
            break;
        case 0x00000003:
            System.out.println(String.format("[%s -> %s]\t[0x%016x] END", sender, receiver, streamId));
            break;
        }
    }

    private void handleThrottle(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        final long streamId = buffer.getLong(index);

        switch (msgTypeId)
        {
        case 0x40000001:
            System.out.println(String.format("[%s <- %s]\t[0x%016x] RESET", sender, receiver, streamId));
            break;
        case 0x40000002:
            final int update = buffer.getInt(index + SIZE_OF_LONG);
            System.out.println(String.format("[%s <- %s]\t[0x%016x] WINDOW (%d)", sender, receiver, streamId, update));
            break;
        }
    }
}
