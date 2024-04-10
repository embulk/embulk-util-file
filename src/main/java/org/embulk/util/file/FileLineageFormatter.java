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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Formats file lineage metadata, given as {@code Map<String, String>}, into a {@link java.lang.String} with a template.
 *
 * <p>It is a helper to format file lineage metadata into a string to fill into columns in records.
 * File lineage metadata is metadata of source files passed from a File Input plugin to Decoder/Parser plugins.
 *
 * <p>An example of file lineage metadata is like below:
 *
 * <pre>{@code {
 *   "bucket": "example-s3-bucket",
 *   "objectKey": "foo/barbaz.csv",
 * }}</pre>
 *
 * <p>By a template string {@code "s3://{bucket}/{objectKey}"}, the example file lineage metadata is formatted
 * into {@code "s3://example-s3-bucket/foo/barbaz.csv"}.
 *
 * <h3><a name="patterns">Patterns and Their Interpretation</a></h3>
 *
 * {@code FileLineageFormatter} uses patterns of the following form:
 *
 * <blockquote><pre> <i>FileLineageFormatterPattern:</i>
 *         <i>String</i>
 *         <i>FileLineageFormatterPattern</i> '{'<i>ParameterKey</i>'}' <i>String</i>
 *
 * <i>ParameterKey:</i>
 *         'a' - 'z'
 *         'A' - 'Z'
 *         '0' - '9'
 *         '_'</pre></blockquote>
 */
public final class FileLineageFormatter {
    private FileLineageFormatter(final ArrayList<Token> tokens) {
        this.tokens = Collections.unmodifiableList(tokens);
    }

    /**
     * Creates a {@link FileLineageFormatter} with a template format.
     *
     * @param format  template format
     * @return formatter
     * @throws IllegalArgumentException  if the specified template format is invalid
     */
    public static FileLineageFormatter from(final String format) {
        return new FileLineageFormatter(parse(format));
    }

    /**
     * Formats a specified file lineage metadata with this formatter.
     *
     * <p>When {@code arguments} do not contain an argument required in the template format, {@code "(null)"} is filled instead.
     *
     * @param arguments  file lineage metadata
     * @return formatted string
     */
    public String format(final Map<String, String> arguments) {
        final StringBuilder builder = new StringBuilder();
        for (final Token token : this.tokens) {
            builder.append(token.format(arguments));
        }
        return builder.toString();
    }

    private static ArrayList<Token> parse(final String format) {
        final ArrayList<Token> tokens = new ArrayList<>();

        StringBuilder currentToken = new StringBuilder();
        boolean inTemplate = false;
        boolean inQuote = false;

        for (int i = 0; i < format.length(); ++i) {
            final char c = format.charAt(i);
            if (inTemplate) {  // in "{key}"
                if (c == '}') {
                    inTemplate = false;
                    // It intentionally allows an empty parameter "{}".
                    tokens.add(new TemplateToken(currentToken.toString()));
                    currentToken = new StringBuilder();
                } else if (isValidCharAsParameter(c)) {
                    currentToken.append(c);
                } else {
                    throw new IllegalArgumentException("Template parameter in the format contains an invalid char at index: " + i);
                }
            } else {  // out of "{key}"
                if (c == '\'') {
                    if (i + 1 < format.length() && format.charAt(i + 1) == '\'') {
                        currentToken.append('\'');
                        ++i;
                    } else {
                        inQuote = !inQuote;
                    }
                } else if (c == '{' && !inQuote) {
                    inTemplate = true;
                    if (currentToken.length() > 0) {
                        tokens.add(new RawToken(currentToken.toString()));
                        currentToken = new StringBuilder();
                    }
                } else {
                    currentToken.append(c);
                }
            }
        }

        if (inTemplate) {
            throw new IllegalArgumentException("Unmatched brace in the format.");
        }
        if (inQuote) {
            throw new IllegalArgumentException("Unmatched quote in the format.");
        }
        if (currentToken.length() > 0) {
            tokens.add(new RawToken(currentToken.toString()));
        }
        return tokens;
    }

    /**
     * Returns {@code true} if {@code c} is valid as a template parameter.
     *
     * <p>It intentionally compares directly with 'a', 'z', 'A', 'Z', '0', '9', and '_' so that it is never affected by locale.
     */
    private static boolean isValidCharAsParameter(final char c) {
        return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '_';
    }

    private abstract static class Token {
        abstract String format(Map<String, String> arguments);
    }

    private static final class RawToken extends Token {
        RawToken(final String raw) {
            this.raw = raw;
        }

        @Override
        String format(Map<String, String> arguments) {
            return this.raw;
        }

        private final String raw;
    }

    private static final class TemplateToken extends Token {
        TemplateToken(final String parameter) {
            this.parameter = parameter;
        }

        @Override
        String format(Map<String, String> arguments) {
            final String value = arguments.get(this.parameter);
            if (value == null) {
                return "(null)";
            }
            return value;
        }

        private final String parameter;
    }

    private final List<Token> tokens;
}
