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
    private Iterator<? extends Iterable<Buffer>> files;
    private Iterator<Buffer> currentBuffers;

    public ListFileInput(Iterable<? extends Iterable<Buffer>> files) {
        this.files = files.iterator();
    }

    public boolean nextFile() {
        if (!files.hasNext()) {
            return false;
        }
        currentBuffers = files.next().iterator();
        return true;
    }

    public Buffer poll() {
        if (currentBuffers == null) {
            throw new IllegalStateException("FileInput.nextFile is not called");
        }
        if (!currentBuffers.hasNext()) {
            return null;
        }
        return currentBuffers.next();
    }

    public void close() {
        do {
            while (true) {
                Buffer b = poll();
                if (b == null) {
                    break;
                }
                b.release();
            }
        } while (nextFile());
    }
}
