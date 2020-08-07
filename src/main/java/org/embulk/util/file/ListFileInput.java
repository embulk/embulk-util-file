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

import java.util.Iterator;
import org.embulk.spi.Buffer;
import org.embulk.spi.FileInput;

public class ListFileInput implements FileInput {
    public ListFileInput(final Iterable<? extends Iterable<Buffer>> files) {
        this.files = files.iterator();
    }

    @Override
    public boolean nextFile() {
        if (!this.files.hasNext()) {
            return false;
        }
        this.currentBuffers = this.files.next().iterator();
        return true;
    }

    @Override
    public Buffer poll() {
        if (this.currentBuffers == null) {
            throw new IllegalStateException("FileInput.nextFile is not called");
        }
        if (!this.currentBuffers.hasNext()) {
            return null;
        }
        return this.currentBuffers.next();
    }

    @Override
    public void close() {
        do {
            while (true) {
                final Buffer b = this.poll();
                if (b == null) {
                    break;
                }
                b.release();
            }
        } while (this.nextFile());
    }

    private Iterator<Buffer> currentBuffers;

    private final Iterator<? extends Iterable<Buffer>> files;
}
