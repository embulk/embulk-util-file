/*
 * Copyright 2021 The Embulk project
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class TestFileLineageFormatter {
    @Test
    public void testEmpty() {
        assertFormat("", "");
    }

    @Test
    public void testQuote() {
        assertFormat("{", "'{'");
        assertFormat("{'}", "'{''}'");
    }

    @Test
    public void testSingle() {
        assertFormat("(null)", "{foo}", "", "bar");
        assertFormat("bar", "{foo}", "foo", "bar");
        assertFormat("bar", "{}", "", "bar");
        assertFormat("(null)", "{}", "foo", "bar");
    }

    @Test
    public void testComplex() {
        assertFormat("{bar}{bar}test2", "{foo}{foo}{bar}", "foo", "{bar}", "bar", "test2");
    }

    @Test
    public void testInvalidFormats() {
        assertInvalidFormat("Unmatched brace in the format.", "{");
        assertInvalidFormat("Unmatched quote in the format.", "'");
        assertInvalidFormat("Unmatched quote in the format.", "'{");
        assertInvalidFormat("Unmatched brace in the format.", "foobar{foo");
        assertInvalidFormat("Template parameter in the format contains an invalid char at index: 7", "foobar{'");
        assertInvalidFormat("Template parameter in the format contains an invalid char at index: 1", "{{}}");
        assertInvalidFormat("Template parameter in the format contains an invalid char at index: 1", "{''}");
    }

    private static void assertInvalidFormat(final String exceptionMessage, final String format) {
        try {
            FileLineageFormatter.from(format);
        } catch (final IllegalArgumentException ex) {
            assertEquals(exceptionMessage, ex.getMessage());
            return;
        }
        fail("IllegalArgumentException is not thrown.");
    }

    private static void assertFormat(final String expected, final String format, final String... arguments) {
        assertEquals(0, arguments.length % 2);
        final HashMap<String, String> argumentsMap = new HashMap<>();
        for (int i = 0; i < arguments.length; i += 2) {
            argumentsMap.put(arguments[i], arguments[i + 1]);
        }

        final FileLineageFormatter formatter = FileLineageFormatter.from(format);
        assertEquals(expected, formatter.format(argumentsMap));
    }
}
