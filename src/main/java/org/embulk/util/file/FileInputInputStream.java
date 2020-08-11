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

import java.io.InputStream;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferImpl;
import org.embulk.spi.FileInput;

public class FileInputInputStream extends InputStream {
    public FileInputInputStream(final FileInput in) {
        this.pos = 0;
        this.buffer = BufferImpl.EMPTY;

        this.in = in;
    }

    public boolean nextFile() {
        this.releaseBuffer();
        return this.in.nextFile();
    }

    @SuppressWarnings("deprecation")  // Calling Buffer#array().
    @Override
    public int read() {
        while (this.pos >= this.buffer.limit()) {
            if (!this.nextBuffer()) {
                return -1;
            }
        }
        final byte b = this.buffer.array()[this.buffer.offset() + this.pos];
        this.pos++;
        if (this.pos >= this.buffer.limit()) {
            this.releaseBuffer();
        }
        return b & 0xff;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) {
        while (this.pos >= this.buffer.limit()) {
            if (!this.nextBuffer()) {
                return -1;
            }
        }
        final int remaining = this.buffer.limit() - this.pos;
        final boolean allConsumed;
        final int lengthToRead;
        if (remaining <= len) {
            allConsumed = true;
            lengthToRead = remaining;
        } else {
            allConsumed = false;
            lengthToRead = len;
        }
        if (b != null) {
            // b == null if skip
            this.buffer.getBytes(this.pos, b, off, lengthToRead);
        }
        if (allConsumed) {
            this.releaseBuffer();
        } else {
            this.pos += lengthToRead;
        }
        return lengthToRead;
    }

    @Override
    public long skip(final long len) {
        final int skipped = this.read(null, 0, (int) Math.min(len, Integer.MAX_VALUE));
        return skipped > 0 ? skipped : 0;
    }

    @Override
    public int available() {
        return this.buffer.limit() - this.pos;
    }

    @Override
    public void close() {
        this.releaseBuffer();
        this.in.close();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    private boolean nextBuffer() {
        this.releaseBuffer();
        final Buffer b = this.in.poll();
        if (b == null) {
            return false;
        }
        this.buffer = b;
        return true;
    }

    private void releaseBuffer() {
        this.buffer.release();
        this.buffer = BufferImpl.EMPTY;
        this.pos = 0;
    }

    private int pos;
    private Buffer buffer;

    private final FileInput in;
}
