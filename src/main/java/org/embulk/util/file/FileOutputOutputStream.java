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
import org.embulk.spi.FileOutput;

public class FileOutputOutputStream extends OutputStream {
    public FileOutputOutputStream(final FileOutput out, final BufferAllocator allocator, final CloseMode closeMode) {
        this.pos = 0;
        this.buffer = allocator.allocate();

        this.out = out;
        this.allocator = allocator;
        this.closeMode = closeMode;
    }

    public enum CloseMode {
        FLUSH,
        FLUSH_FINISH,
        FLUSH_FINISH_CLOSE,
        CLOSE,
        ;
    }

    public void nextFile() {
        this.out.nextFile();
    }

    public void finish() {
        this.doFlush();
        this.out.finish();
    }

    @SuppressWarnings("deprecation")  // Calling Buffer#array().
    @Override
    public void write(final int b) {
        this.buffer.array()[this.buffer.offset() + this.pos] = (byte) b;
        this.pos++;
        if (this.pos >= this.buffer.capacity()) {
            this.flush();
        }
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
        int cursor = off;
        int lengthRemaining = len;

        while (true) {
            final int available = this.buffer.capacity() - this.pos;
            if (available < lengthRemaining) {
                this.buffer.setBytes(this.pos, b, cursor, available);
                this.pos += available;
                lengthRemaining -= available;
                cursor += available;
                this.flush();
            } else {
                this.buffer.setBytes(this.pos, b, cursor, lengthRemaining);
                this.pos += lengthRemaining;
                if (available <= lengthRemaining) {
                    this.flush();
                }
                break;
            }
        }
    }

    private boolean doFlush() {
        if (this.pos > 0) {
            this.buffer.limit(this.pos);
            this.out.add(this.buffer);
            this.buffer = EmptyBuffer.getInstance();
            this.pos = 0;
            return true;
        }
        return false;
    }

    @Override
    public void flush() {
        if (this.doFlush()) {
            this.buffer = this.allocator.allocate();
        }
    }

    @Override
    public void close() {
        switch (this.closeMode) {
            case FLUSH:
                this.doFlush();
                break;
            case FLUSH_FINISH:
                this.doFlush();
                this.out.finish();
                break;
            case FLUSH_FINISH_CLOSE:
                this.doFlush();
                this.out.finish();
                this.out.close();
                break;
            case CLOSE:
                this.out.close();
                break;
            default:  // Never default as all enums are listed.
        }
        this.buffer.release();
        this.buffer = EmptyBuffer.getInstance();
        this.pos = 0;
    }

    private int pos;
    private Buffer buffer;

    private final FileOutput out;
    private final BufferAllocator allocator;
    private final CloseMode closeMode;
}
