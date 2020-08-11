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

import java.io.InputStream;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.TransactionalFileInput;

public abstract class InputStreamTransactionalFileInput extends InputStreamFileInput implements TransactionalFileInput {
    public InputStreamTransactionalFileInput(final BufferAllocator allocator, final Provider provider) {
        super(allocator, provider);
    }

    public InputStreamTransactionalFileInput(final BufferAllocator allocator, final Opener opener) {
        super(allocator, opener);
    }

    public InputStreamTransactionalFileInput(final BufferAllocator allocator, final InputStream openedStream) {
        super(allocator, openedStream);
    }
}
