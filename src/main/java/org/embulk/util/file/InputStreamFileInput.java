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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Optional;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.FileInput;

public class InputStreamFileInput implements FileInput {
    public InputStreamFileInput(final BufferAllocator allocator, final Provider provider) {
        this.current = null;

        this.allocator = allocator;
        this.provider = provider;
    }

    public InputStreamFileInput(final BufferAllocator allocator, final Opener opener) {
        this(allocator, new OpenerProvider(opener));
    }

    public InputStreamFileInput(final BufferAllocator allocator, final InputStream openedStream) {
        this(allocator, new InputStreamProvider(openedStream));
    }

    public interface Provider extends Closeable {
        default InputStreamWithHints openNextWithHints() throws IOException {
            return new InputStreamWithHints(this.openNext());
        }

        default InputStream openNext() throws IOException {
            throw new UnsupportedOperationException(
                    "InputStreamFileInput.Provider#openNext must be implemented"
                    + " unless InputStreamFileInput.Provider#openNextWithHints is implemented.");
        }

        void close() throws IOException;
    }

    public interface Opener {
        InputStream open() throws IOException;
    }

    public static class IteratorProvider implements Provider {
        public IteratorProvider(final Iterable<InputStream> iterable) {
            this.iterator = iterable.iterator();
        }

        public IteratorProvider(final Iterator<InputStream> iterator) {
            this.iterator = iterator;
        }

        @Override
        public InputStream openNext() throws IOException {
            if (!this.iterator.hasNext()) {
                return null;
            }
            return this.iterator.next();
        }

        @Override
        public void close() throws IOException {
            while (this.iterator.hasNext()) {
                this.iterator.next().close();
            }
        }

        private final Iterator<InputStream> iterator;
    }

    public static class InputStreamWithHints {
        public InputStreamWithHints(final InputStream inputStream, final String hintOfCurrentInputFileNameForLogging) {
            this.inputStream = inputStream;
            this.hintOfCurrentInputFileNameForLogging = Optional.ofNullable(hintOfCurrentInputFileNameForLogging);
        }

        public InputStreamWithHints(final InputStream inputStream) {
            this.inputStream = inputStream;
            this.hintOfCurrentInputFileNameForLogging = Optional.empty();
        }

        public InputStream getInputStream() {
            return this.inputStream;
        }

        public Optional<String> getHintOfCurrentInputFileNameForLogging() {
            return this.hintOfCurrentInputFileNameForLogging;
        }

        private final InputStream inputStream;
        private final Optional<String> hintOfCurrentInputFileNameForLogging;
    }

    @Override
    public boolean nextFile() {
        try {
            if (this.current != null && this.current.getInputStream() != null) {
                this.current.getInputStream().close();
                this.current = null;
            }
            this.current = this.provider.openNextWithHints();
            return this.current != null && this.current.getInputStream() != null;
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @SuppressWarnings("deprecation")  // Calling Buffer#array().
    @Override
    public Buffer poll() {
        if (this.current == null || this.current.getInputStream() == null) {
            throw new IllegalStateException("InputStreamFileInput#nextFile() must be called before poll().");
        }
        // TODO: Clean it up and "final".
        Buffer buffer = this.allocator.allocate();
        try {
            final int n = this.current.getInputStream().read(buffer.array(), buffer.offset(), buffer.capacity());
            if (n < 0) {
                return null;
            }
            buffer.limit(n);
            Buffer b = buffer;
            buffer = null;
            return b;
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        } finally {
            if (buffer != null) {
                buffer.release();
                buffer = null;
            }
        }
    }

    @Override
    public void close() {
        try {
            try {
                if (this.current != null && this.current.getInputStream() != null) {
                    this.current.getInputStream().close();
                    this.current = null;
                }
            } finally {
                this.provider.close();
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public Optional<String> hintOfCurrentInputFileNameForLogging() {
        return this.getHintOfCurrentInputFileNameForLogging();
    }

    protected final Optional<String> getHintOfCurrentInputFileNameForLogging() {
        if (this.current != null) {
            return this.current.getHintOfCurrentInputFileNameForLogging();
        } else {
            return Optional.empty();
        }
    }

    private static class OpenerProvider implements Provider {
        public OpenerProvider(final Opener opener) {
            this.opener = opener;
        }

        @Override
        public InputStream openNext() throws IOException {
            if (this.opener == null) {
                return null;
            }
            final InputStream stream = this.opener.open();
            this.opener = null;
            return stream;
        }

        @Override
        public void close() throws IOException {}

        private Opener opener;
    }

    private static class InputStreamProvider implements Provider {
        public InputStreamProvider(final InputStream input) {
            this.input = input;
        }

        @Override
        public InputStream openNext() throws IOException {
            if (this.input == null) {
                return null;
            }
            final InputStream ret = this.input;
            this.input = null;
            return ret;
        }

        @Override
        public void close() throws IOException {
            if (this.input != null) {
                this.input.close();
                this.input = null;
            }
        }

        private InputStream input;
    }

    private InputStreamWithHints current;

    private final BufferAllocator allocator;
    private final Provider provider;
}
