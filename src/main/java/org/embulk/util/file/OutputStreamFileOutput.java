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
import org.embulk.spi.Buffer;
import org.embulk.spi.FileOutput;

public class OutputStreamFileOutput implements FileOutput {
    public interface Provider extends Closeable {
        public OutputStream openNext() throws IOException;

        public void finish() throws IOException;

        public void close() throws IOException;
    }

    private final Provider provider;
    private OutputStream current;

    public OutputStreamFileOutput(Provider provider) {
        this.provider = provider;
        this.current = null;
    }

    public void nextFile() {
        closeCurrent();
        try {
            current = provider.openNext();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("deprecation")  // Calling Buffer#array().
    public void add(Buffer buffer) {
        if (current == null) {
            throw new IllegalStateException("nextFile() must be called before poll()");
        }
        try {
            current.write(buffer.array(), buffer.offset(), buffer.limit());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            buffer.release();
        }
    }

    public void finish() {
        closeCurrent();
        try {
            provider.finish();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void close() {
        try {
            closeCurrent();
        } finally {
            try {
                provider.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void closeCurrent() {
        try {
            if (current != null) {
                current.close();
                current = null;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
