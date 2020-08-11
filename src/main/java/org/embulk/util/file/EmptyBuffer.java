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

final class EmptyBuffer extends Buffer {
    EmptyBuffer() {
    }

    @SuppressWarnings("deprecation")
    @Override
    public byte[] array() {
        return EMPTY_BYTES;
    }

    @Override
    public int offset() {
        return 0;
    }

    @Override
    public Buffer offset(final int offset) {
        return this;
    }

    @Override
    public int limit() {
        return 0;
    }

    @Override
    public Buffer limit(final int limit) {
        return this;
    }

    @Override
    public int capacity() {
        return 0;
    }

    @Override
    public void setBytes(int index, byte[] source, int sourceIndex, int length) {
        return;
    }

    @Override
    public void setBytes(int index, Buffer source, int sourceIndex, int length) {
        return;
    }

    @Override
    public void getBytes(int index, byte[] dest, int destIndex, int length) {
        return;
    }

    @Override
    public void getBytes(int index, Buffer dest, int destIndex, int length) {
        return;
    }

    @Override
    public void release() {
        return;
    }

    static final EmptyBuffer INSTANCE = new EmptyBuffer();

    private static final byte[] EMPTY_BYTES = new byte[0];
}
