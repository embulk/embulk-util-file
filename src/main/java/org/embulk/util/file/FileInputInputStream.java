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
    private final FileInput in;
    private int pos;
    private Buffer buffer = BufferImpl.EMPTY;

    public FileInputInputStream(FileInput in) {
        this.in = in;
    }

    public boolean markSupported() {
        return false;
    }

    public boolean nextFile() {
        releaseBuffer();
        return in.nextFile();
    }

    @Override
    public int available() {
        return buffer.limit() - pos;
    }

    @SuppressWarnings("deprecation")  // Calling Buffer#array().
    @Override
    public int read() {
        while (pos >= buffer.limit()) {
            if (!nextBuffer()) {
                return -1;
            }
        }
        byte b = buffer.array()[buffer.offset() + pos];
        pos++;
        if (pos >= buffer.limit()) {
            releaseBuffer();
        }
        return b & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        while (pos >= buffer.limit()) {
            if (!nextBuffer()) {
                return -1;
            }
        }
        int remaining = buffer.limit() - pos;
        boolean allConsumed;
        if (remaining <= len) {
            allConsumed = true;
            len = remaining;
        } else {
            allConsumed = false;
        }
        if (b != null) {
            // b == null if skip
            buffer.getBytes(pos, b, off, len);
        }
        if (allConsumed) {
            releaseBuffer();
        } else {
            pos += len;
        }
        return len;
    }

    @Override
    public long skip(long len) {
        int skipped = read(null, 0, (int) Math.min(len, Integer.MAX_VALUE));
        return skipped > 0 ? skipped : 0;
    }

    private boolean nextBuffer() {
        releaseBuffer();
        Buffer b = in.poll();
        if (b == null) {
            return false;
        }
        buffer = b;
        return true;
    }

    private void releaseBuffer() {
        buffer.release();
        buffer = BufferImpl.EMPTY;
        pos = 0;
    }

    @Override
    public void close() {
        releaseBuffer();
        in.close();
    }
}
