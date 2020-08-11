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

public enum Newline {
    CRLF("\r\n"),
    LF("\n"),
    CR("\r"),
    ;

    private Newline(final String string) {
        this.string = string;
        this.firstCharCode = string.charAt(0);
        if (string.length() > 1) {
            this.secondCharCode = string.charAt(1);
        } else {
            this.secondCharCode = 0;
        }
    }

    public String getString() {
        return this.string;
    }

    public char getFirstCharCode() {
        return this.firstCharCode;
    }

    public char getSecondCharCode() {
        return this.secondCharCode;
    }

    private final String string;
    private final char firstCharCode;
    private final char secondCharCode;
}
