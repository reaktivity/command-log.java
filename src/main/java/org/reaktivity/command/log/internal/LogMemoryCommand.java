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

import static java.lang.Integer.bitCount;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Long.bitCount;
import static java.nio.file.Files.isRegularFile;
import static org.agrona.LangUtil.rethrowUnchecked;
import static org.reaktivity.command.log.internal.layouts.MemoryLayout.BTREE_OFFSET;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.agrona.DirectBuffer;
import org.reaktivity.command.log.internal.layouts.MemoryLayout;
import org.reaktivity.nukleus.Configuration;

public final class LogMemoryCommand
{
    private final Path directory;
    private final boolean verbose;
    private final Logger out;

    public LogMemoryCommand(
        Configuration config,
        Logger out,
        boolean verbose)
    {
        this.directory = config.directory();
        this.out = out;
        this.verbose = verbose;
    }

    private boolean isMemoryFile(
        Path path)
    {
        return path.getNameCount() - directory.getNameCount() == 1 &&
                "memory0".equals(path.getName(path.getNameCount() - 1).toString()) &&
                isRegularFile(path);
    }

   private void displayMemory(
        Path path)
    {
        try (MemoryLayout layout = new MemoryLayout.Builder()
                .path(path)
                .create(false)
                .build();
        )
        {
            String name = path.getName(path.getNameCount() - 1).toString();
            displayMemory(name, layout);
        }
    }

    private void displayMemory(
        String name,
        MemoryLayout layout)
    {
        final int minimumBlockSize = layout.minimumBlockSize();
        final int maximumBlockSize = layout.maximumBlockSize();
        final DirectBuffer metadataBuffer = layout.metadataBuffer();

        final int orderCount = numberOfTrailingZeros(maximumBlockSize) - numberOfTrailingZeros(minimumBlockSize) + 1;
        final int sizeofBTree = metadataBuffer.capacity() - BTREE_OFFSET;
        if (verbose)
        {
            out.printf("%s (%d, %d, %d)\n", name, minimumBlockSize, maximumBlockSize, sizeofBTree);
            out.printf("block\t\tcount\n");
        }
        long memoryUsed = 0L;
        for (int depth=0; depth < orderCount; depth++)
        {
            int length = 1 << depth;
            int offset = length >> 2;
            int order = orderCount - depth - 1;
            int block = minimumBlockSize << order;
            switch (length)
            {
            case 1:
                final byte value1 = metadataBuffer.getByte(offset + BTREE_OFFSET);
                final int bitCount1 = bitCount((value1 & 0x04) & ~((value1 >> 1) & 0x04));
                memoryUsed += bitCount1 * block;
                if (verbose)
                {
                    out.printf("%d\t\t%d\n", block, bitCount1);
                }
                break;
            case 2:
                final byte value2 = metadataBuffer.getByte(offset + BTREE_OFFSET);
                final int bitCount2 = bitCount((value2 & 0x50) & ~(((value2 & 0xa0) >> 1) & 0x50));
                memoryUsed += bitCount2 * block;
                if (verbose)
                {
                    out.printf("%d\t\t%d\n", block, bitCount2);
                }
                break;
            case 4:
                final byte value4 = metadataBuffer.getByte(offset + BTREE_OFFSET);
                final int bitCount4 = bitCount((value4 & 0x55) & ~(((value4 & 0xaa) >> 1) & 0x55));
                memoryUsed += bitCount4 * block;
                if (verbose)
                {
                    out.printf("%d\t\t%d\n", block, bitCount4);
                }
                break;
            case 8:
                final short value8 = metadataBuffer.getShort(offset + BTREE_OFFSET);
                final int bitCount8 = bitCount((value8 & 0x5555) & ~(((value8 & 0xaaaa) >> 1) & 0x5555));
                memoryUsed += bitCount8 * block;
                if (verbose)
                {
                    out.printf("%d\t\t%d\n", block, bitCount8);
                }
                break;
            case 16:
                final int value16 = metadataBuffer.getInt(offset + BTREE_OFFSET);
                final int bitCount16 = bitCount((value16 & 0x5555_5555) & ~(((value16 & 0xaaaa_aaaa) >> 1) & 0x5555_5555));
                memoryUsed += bitCount16 * block;
                if (verbose)
                {
                    out.printf("%d\t\t%d\n", block, bitCount16);
                }
                break;
            default:
                int bitCount32 = 0;
                for (int index = offset, limit=offset + length; offset < limit; index++)
                {
                    final long value32 = metadataBuffer.getLong(index + BTREE_OFFSET);
                    bitCount32 += bitCount((value32 & 0x5555_5555_5555_5555L) &
                                         ~(((value32 & 0xaaaa_aaaa_aaaa_aaaaL) >> 1) & 0x5555_5555_5555_5555L));
                    offset += Long.SIZE;
                }
                memoryUsed += bitCount32 * block;
                if (verbose)
                {
                    out.printf("%d\t\t%d\n", block, bitCount32);
                }
                break;
            }
        }
        if (verbose)
        {
            out.printf("total %d / %d\n", memoryUsed, maximumBlockSize);
        }
        out.printf("%s %.3f%%\n", name, memoryUsed / (float) maximumBlockSize);
    }

    void invoke()
    {
        try (Stream<Path> files = Files.walk(directory, 2))
        {
            files.filter(this::isMemoryFile)
                 .forEach(this::displayMemory);
        }
        catch (IOException ex)
        {
            rethrowUnchecked(ex);
        }
    }
}
