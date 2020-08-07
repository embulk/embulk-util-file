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
import java.io.UncheckedIOException;

public class ResumableInputStream extends InputStream {
    public ResumableInputStream(final InputStream initialInputStream, final Reopener reopener) {
        this.in = initialInputStream;

        this.offset = 0L;
        this.markedOffset = 0L;
        this.lastClosedCause = null;
        this.closed = false;

        this.reopener = reopener;
    }

    public ResumableInputStream(final Reopener reopener) throws IOException {
        this(reopener.reopen(0, null), reopener);
    }

    public interface Reopener {
        InputStream reopen(long offset, Exception closedCause) throws IOException;
    }

    @Override
    public int read() throws IOException {
        this.ensureOpened();
        while (true) {
            try {
                final int v = this.in.read();
                this.offset += 1;
                return v;
            } catch (final IOException | RuntimeException ex) {
                this.reopen(ex);
            }
        }
    }

    @Override
    public int read(final byte[] b) throws IOException {
        this.ensureOpened();
        while (true) {
            try {
                final int r = in.read(b);
                this.offset += r;
                return r;
            } catch (final IOException | RuntimeException ex) {
                this.reopen(ex);
            }
        }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        this.ensureOpened();
        while (true) {
            try {
                final int r = in.read(b, off, len);
                this.offset += r;
                return r;
            } catch (final IOException | RuntimeException ex) {
                this.reopen(ex);
            }
        }
    }

    @Override
    public long skip(final long n) throws IOException {
        this.ensureOpened();
        while (true) {
            try {
                final long r = in.skip(n);
                this.offset += r;
                return r;
            } catch (final IOException | RuntimeException ex) {
                this.reopen(ex);
            }
        }
    }

    @Override
    public int available() throws IOException {
        this.ensureOpened();
        return this.in.available();
    }

    @Override
    public void close() throws IOException {
        if (this.in != null) {
            this.in.close();
            this.closed = true;
            this.in = null;
        }
    }

    @Override
    public void mark(final int readlimit) {
        try {
            this.ensureOpened();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        this.in.mark(readlimit);
        this.markedOffset = this.offset;
    }

    @Override
    public void reset() throws IOException {
        this.ensureOpened();
        this.in.reset();
        this.offset = this.markedOffset;
    }

    @Override
    public boolean markSupported() {
        try {
            this.ensureOpened();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return this.in.markSupported();
    }

    private void reopen(final Exception closedCause) throws IOException {
        if (this.in != null) {
            this.lastClosedCause = closedCause;
            try {
                this.in.close();
            } catch (final IOException ignored) {
                // Passing through intentionally.
            }
            this.in = null;
        }
        this.in = this.reopener.reopen(this.offset, closedCause);
        this.lastClosedCause = null;
    }

    private void ensureOpened() throws IOException {
        if (this.in == null) {
            if (this.closed) {
                throw new IOException("stream closed");
            }
            this.reopen(this.lastClosedCause);
        }
    }

    protected InputStream in;

    private long offset;
    private long markedOffset;
    private Exception lastClosedCause;
    private boolean closed;

    private final Reopener reopener;
}
