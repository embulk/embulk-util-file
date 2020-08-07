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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import org.embulk.spi.Buffer;
import org.embulk.spi.FileOutput;

public class OutputStreamFileOutput implements FileOutput {
    public OutputStreamFileOutput(final Provider provider) {
        this.current = null;

        this.provider = provider;
    }

    public interface Provider extends Closeable {
        OutputStream openNext() throws IOException;

        void finish() throws IOException;

        void close() throws IOException;
    }

    @Override
    public void nextFile() {
        this.closeCurrent();
        try {
            this.current = this.provider.openNext();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @SuppressWarnings("deprecation")  // Calling Buffer#array().
    @Override
    public void add(final Buffer buffer) {
        if (this.current == null) {
            throw new IllegalStateException("nextFile() must be called before poll()");
        }
        try {
            this.current.write(buffer.array(), buffer.offset(), buffer.limit());
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        } finally {
            buffer.release();
        }
    }

    @Override
    public void finish() {
        this.closeCurrent();
        try {
            this.provider.finish();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void close() {
        try {
            this.closeCurrent();
        } finally {
            try {
                this.provider.close();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    private void closeCurrent() {
        try {
            if (this.current != null) {
                this.current.close();
                this.current = null;
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private OutputStream current;

    private final Provider provider;
}
