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
import java.util.Iterator;
import java.util.Optional;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.FileInput;

public class InputStreamFileInput implements FileInput {
    public InputStreamFileInput(BufferAllocator allocator, Provider provider) {
        this.current = null;

        this.allocator = allocator;
        this.provider = provider;
    }

    public InputStreamFileInput(BufferAllocator allocator, Opener opener) {
        this(allocator, new OpenerProvider(opener));
    }

    public InputStreamFileInput(BufferAllocator allocator, InputStream openedStream) {
        this(allocator, new InputStreamProvider(openedStream));
    }

    public interface Provider extends Closeable {
        default InputStreamWithHints openNextWithHints() throws IOException {
            return new InputStreamWithHints(this.openNext());
        }

        default InputStream openNext() throws IOException {
            throw new UnsupportedOperationException(
                    "Provider#openNext must be implemented unless Provider#openNextWithHints is implemented.");
        }

        public void close() throws IOException;
    }

    public interface Opener {
        public InputStream open() throws IOException;
    }

    public static class IteratorProvider implements Provider {
        public IteratorProvider(Iterable<InputStream> iterable) {
            this.iterator = iterable.iterator();
        }

        public IteratorProvider(Iterator<InputStream> iterator) {
            this.iterator = iterator;
        }

        @Override
        public InputStream openNext() throws IOException {
            if (!iterator.hasNext()) {
                return null;
            }
            return iterator.next();
        }

        @Override
        public void close() throws IOException {
            while (iterator.hasNext()) {
                iterator.next().close();
            }
        }

        private Iterator<InputStream> iterator;
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
            if (current != null && current.getInputStream() != null) {
                current.getInputStream().close();
                current = null;
            }
            current = provider.openNextWithHints();
            return current != null && current.getInputStream() != null;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("deprecation")  // Calling Buffer#array().
    @Override
    public Buffer poll() {
        if (current == null || current.getInputStream() == null) {
            throw new IllegalStateException("nextFile() must be called before poll()");
        }
        Buffer buffer = allocator.allocate();
        try {
            int n = current.getInputStream().read(buffer.array(), buffer.offset(), buffer.capacity());
            if (n < 0) {
                return null;
            }
            buffer.limit(n);
            Buffer b = buffer;
            buffer = null;
            return b;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
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
                if (current != null && current.getInputStream() != null) {
                    current.getInputStream().close();
                    current = null;
                }
            } finally {
                provider.close();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Optional<String> hintOfCurrentInputFileNameForLogging() {
        return this.getHintOfCurrentInputFileNameForLogging();
    }

    protected final Optional<String> getHintOfCurrentInputFileNameForLogging() {
        if (current != null) {
            return current.getHintOfCurrentInputFileNameForLogging();
        } else {
            return Optional.empty();
        }
    }

    private static class OpenerProvider implements Provider {
        public OpenerProvider(Opener opener) {
            this.opener = opener;
        }

        @Override
        public InputStream openNext() throws IOException {
            if (opener == null) {
                return null;
            }
            InputStream stream = opener.open();
            opener = null;
            return stream;
        }

        @Override
        public void close() throws IOException {}

        private Opener opener;
    }

    private static class InputStreamProvider implements Provider {
        public InputStreamProvider(InputStream input) {
            this.input = input;
        }

        @Override
        public InputStream openNext() throws IOException {
            if (input == null) {
                return null;
            }
            InputStream ret = input;
            input = null;
            return ret;
        }

        @Override
        public void close() throws IOException {
            if (input != null) {
                input.close();
                input = null;
            }
        }

        private InputStream input;
    }

    private InputStreamWithHints current;

    private final BufferAllocator allocator;
    private final Provider provider;
}
