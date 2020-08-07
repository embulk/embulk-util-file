/*
 * Copyright 2014 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.util.file;

import java.io.OutputStream;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.BufferImpl;
import org.embulk.spi.FileOutput;

public class FileOutputOutputStream extends OutputStream {
    public FileOutputOutputStream(FileOutput out, BufferAllocator allocator, CloseMode closeMode) {
        this.buffer = allocator.allocate();

        this.out = out;
        this.allocator = allocator;
        this.closeMode = closeMode;
    }

    public static enum CloseMode {
        FLUSH,
        FLUSH_FINISH,
        FLUSH_FINISH_CLOSE,
        CLOSE,
        ;
    }

    public void nextFile() {
        out.nextFile();
    }

    public void finish() {
        doFlush();
        out.finish();
    }

    @SuppressWarnings("deprecation")  // Calling Buffer#array().
    @Override
    public void write(int b) {
        buffer.array()[buffer.offset() + pos] = (byte) b;
        pos++;
        if (pos >= buffer.capacity()) {
            flush();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) {
        while (true) {
            int available = buffer.capacity() - pos;
            if (available < len) {
                buffer.setBytes(pos, b, off, available);
                pos += available;
                len -= available;
                off += available;
                flush();
            } else {
                buffer.setBytes(pos, b, off, len);
                pos += len;
                if (available <= len) {
                    flush();
                }
                break;
            }
        }
    }

    private boolean doFlush() {
        if (pos > 0) {
            buffer.limit(pos);
            out.add(buffer);
            buffer = BufferImpl.EMPTY;
            pos = 0;
            return true;
        }
        return false;
    }

    @Override
    public void flush() {
        if (doFlush()) {
            buffer = allocator.allocate();
        }
    }

    @Override
    public void close() {
        switch (closeMode) {
            case FLUSH:
                doFlush();
                break;
            case FLUSH_FINISH:
                doFlush();
                out.finish();
                break;
            case FLUSH_FINISH_CLOSE:
                doFlush();
                out.finish();
                out.close();
                break;
            case CLOSE:
                out.close();
                break;
            default:  // Never default as all enums are listed.
        }
        buffer.release();
        buffer = BufferImpl.EMPTY;
        pos = 0;
    }

    private int pos;
    private Buffer buffer;

    private final FileOutput out;
    private final BufferAllocator allocator;
    private final CloseMode closeMode;
}
