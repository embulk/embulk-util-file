/*
 * Copyright 2020 The Embulk project
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

import org.embulk.spi.Buffer;

final class EmptyBuffer {
    private EmptyBuffer() {}

    static Buffer getInstance() {
        return Holder.INSTANCE;
    }

    private static class Holder {  // Initialization-on-demand holder idiom.
        private static final Buffer INSTANCE = createInstance();

        private static Buffer createInstance() {
            // EmptyBufferCompat and EmptyBufferUpToDate are implemented as separate independent classes while
            // they have the same methods so that this library can work both with Embulk v0.9 and v0.10.
            //
            // EmptyBufferUpToDate depends on the constructor |Buffer()| of the latest abstract org.embulk.spi.Buffer.
            // https://github.com/embulk/embulk/blob/v0.10.27/embulk-api/src/main/java/org/embulk/spi/Buffer.java#L38-L39
            //
            // It is basically recommended in Embulk v0.10 and later, but it did not exist in Embulk v0.9 or earlier.
            // Embulk v0.9 had only the constructor |Buffer(byte[], int, int)| of org.embulk.spi.Buffer.
            // https://github.com/embulk/embulk/blob/v0.9.23/embulk-core/src/main/java/org/embulk/spi/Buffer.java
            //
            // EmptyBufferCompat is implemented to depend on |Buffer(byte[], int, int)| to work with Embulk v0.9.
            //
            // They are implemented as separate independent classes because one can fail when the class is initialized
            // if its constructor is depending on a non-existing super-class constructor. They would never fail unless
            // the class is initialized / loaded.
            try {
                Buffer.class.getConstructor();  // throws NoSuchMethodException if working with v0.9.
                return new EmptyBufferUpToDate();
            } catch (final NoSuchMethodException ex) {
                // Pass-through.
            }

            return new EmptyBufferCompat();
        }
    }
}
