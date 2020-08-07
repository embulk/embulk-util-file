/*
 * Copyright 2015 The Embulk project
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

import java.io.IOException;
import java.io.InputStream;

public class ResumableInputStream extends InputStream {
    public ResumableInputStream(InputStream initialInputStream, Reopener reopener) {
        this.in = initialInputStream;

        this.offset = 0L;
        this.markedOffset = 0L;
        this.lastClosedCause = null;

        this.reopener = reopener;
    }

    public ResumableInputStream(Reopener reopener) throws IOException {
        this(reopener.reopen(0, null), reopener);
    }

    public interface Reopener {
        public InputStream reopen(long offset, Exception closedCause) throws IOException;
    }

    @Override
    public int read() throws IOException {
        ensureOpened();
        while (true) {
            try {
                int v = in.read();
                offset += 1;
                return v;
            } catch (IOException | RuntimeException ex) {
                reopen(ex);
            }
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        ensureOpened();
        while (true) {
            try {
                int r = in.read(b);
                offset += r;
                return r;
            } catch (IOException | RuntimeException ex) {
                reopen(ex);
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpened();
        while (true) {
            try {
                int r = in.read(b, off, len);
                offset += r;
                return r;
            } catch (IOException | RuntimeException ex) {
                reopen(ex);
            }
        }
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpened();
        while (true) {
            try {
                long r = in.skip(n);
                offset += r;
                return r;
            } catch (IOException | RuntimeException ex) {
                reopen(ex);
            }
        }
    }

    @Override
    public int available() throws IOException {
        ensureOpened();
        return in.available();
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            closed = true;
            in = null;
        }
    }

    @Override
    public void mark(int readlimit) {
        try {
            ensureOpened();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        in.mark(readlimit);
        markedOffset = offset;
    }

    @Override
    public void reset() throws IOException {
        ensureOpened();
        in.reset();
        offset = markedOffset;
    }

    @Override
    public boolean markSupported() {
        try {
            ensureOpened();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return in.markSupported();
    }

    private void reopen(Exception closedCause) throws IOException {
        if (in != null) {
            lastClosedCause = closedCause;
            try {
                in.close();
            } catch (IOException ignored) {
                // Passing through intentionally.
            }
            in = null;
        }
        in = reopener.reopen(offset, closedCause);
        lastClosedCause = null;
    }

    private void ensureOpened() throws IOException {
        if (in == null) {
            if (closed) {
                throw new IOException("stream closed");
            }
            reopen(lastClosedCause);
        }
    }

    protected InputStream in;

    private long offset;
    private long markedOffset;
    private Exception lastClosedCause;
    private boolean closed;

    private final Reopener reopener;
}
