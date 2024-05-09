/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polycat.catalog.common.utils;

import io.polycat.catalog.common.constants.CompatibleHiveConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.polycat.catalog.common.utils.ConfUtil.StructuredOptionsSplitter.escapeWithSingleQuote;
import static io.polycat.catalog.common.utils.Preconditions.checkNotNull;

@Slf4j
public class ConfUtil {

    private static final Map<String, String> CATALOG_NAME_MAPPING = new HashMap<>();

    static {
        // catalog.hive.catalog.names.mapping=hive:alias1|alias2,hive1:alias3|alias4,hive1:alias5|alias5,
        HiveConf conf = new HiveConf();
        initCatalogNameMapping(conf);
    }

    public static String getCatalogMappingName(String hiveCatalogName) {
        return CATALOG_NAME_MAPPING.getOrDefault(hiveCatalogName, hiveCatalogName);
    }

    public static boolean hasMappingName(String hiveCatalogName) {
        return CATALOG_NAME_MAPPING.containsKey(hiveCatalogName);
    }

    public static Set<String> getMappingKeys() {
        return CATALOG_NAME_MAPPING.keySet();
    }

    private static void initCatalogNameMapping(HiveConf conf) {
        String catalogNameMappingStr = conf.get(CompatibleHiveConstants.V3_CONF_CATALOG_NAME_MAPPING);
        try {
            if (catalogNameMappingStr != null && catalogNameMappingStr.length() > 0) {
                String[] catalogs = catalogNameMappingStr.split(",");
                for (int i = 0; i < catalogs.length; i++) {
                    if (catalogs[i].length() > 0) {
                        String[] kvMap = catalogs[i].split(":");
                        if (kvMap.length != 2) {
                            throw new IllegalArgumentException("Param illegal: " + CompatibleHiveConstants.V3_CONF_CATALOG_NAME_MAPPING + ", e.g: hive:alias1|alias2,hive1:alias3|alias4");
                        }
                        if (kvMap[1].length() > 0) {
                            String[] hiveCatalogNames = kvMap[1].split("\\|");
                            for (int j = 0; j < hiveCatalogNames.length; j++) {
                                if (CATALOG_NAME_MAPPING.containsKey(hiveCatalogNames[j])) {
                                    throw new IllegalArgumentException("Param illegal: " + CompatibleHiveConstants.V3_CONF_CATALOG_NAME_MAPPING + ", The mapping relationship is duplicated: " + catalogNameMappingStr);
                                }
                                CATALOG_NAME_MAPPING.put(hiveCatalogNames[j], kvMap[0]);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Param config error: " + CompatibleHiveConstants.V3_CONF_CATALOG_NAME_MAPPING + " = " + catalogNameMappingStr, e);
        }
    }

    public static Map<String, String> convertToPropertiesViaPrefix(
            Map<String, String> propMap, final String prefix) {
        return propMap.keySet().stream()
                .filter(k -> k.startsWith(prefix))
                .collect(
                        Collectors.toMap(
                                k -> k.substring(prefix.length()),
                                propMap::get));
    }

    public static String getMapPropVal(Map<String, String> properties, String propKey) {
        if (properties != null && !properties.isEmpty()) {
            return properties.get(propKey);
        }
        return null;
    }

    /**
     * Tries to convert the raw value into the provided type.
     *
     * @param rawValue rawValue to convert into the provided type clazz
     * @param clazz clazz specifying the target type
     * @param <T> type of the result
     * @return the converted value if rawValue is of type clazz
     * @throws IllegalArgumentException if the rawValue cannot be converted in the specified target
     *     type clazz
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object rawValue, Class<?> clazz) {
        if (rawValue == null) {
            return null;
        }
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(rawValue);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(rawValue, (Class<? extends Enum<?>>) clazz);
        } else if (clazz == Map.class) {
            return (T) convertToProperties(rawValue);
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    @SuppressWarnings("unchecked")
    static Map<String, String> convertToProperties(Object o) {
        if (o instanceof Map) {
            return (Map<String, String>) o;
        } else {
            List<String> listOfRawProperties =
                    StructuredOptionsSplitter.splitEscaped(o.toString(), ',');
            return listOfRawProperties.stream()
                    .map(s -> StructuredOptionsSplitter.splitEscaped(s, ':'))
                    .peek(
                            pair -> {
                                if (pair.size() != 2) {
                                    throw new IllegalArgumentException(
                                            "Map item is not a key-value pair (missing ':'?)");
                                }
                            })
                    .collect(Collectors.toMap(a -> a.get(0), a -> a.get(1)));
        }
    }

    @SuppressWarnings("unchecked")
    public static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        if (o.getClass().equals(clazz)) {
            return (E) o;
        }

        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    static String convertToString(Object o) {
        if (o.getClass() == String.class) {
            return (String) o;
        } else if (o instanceof List) {
            return ((List<?>) o)
                    .stream()
                    .map(e -> escapeWithSingleQuote(convertToString(e), ";"))
                    .collect(Collectors.joining(";"));
        } else if (o instanceof Map) {
            return ((Map<?, ?>) o)
                    .entrySet().stream()
                    .map(
                            e -> {
                                String escapedKey =
                                        escapeWithSingleQuote(e.getKey().toString(), ":");
                                String escapedValue =
                                        escapeWithSingleQuote(e.getValue().toString(), ":");

                                return escapeWithSingleQuote(
                                        escapedKey + ":" + escapedValue, ",");
                            })
                    .collect(Collectors.joining(","));
        }

        return o.toString();
    }

    static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the integer type.",
                                value));
            }
        }

        return Integer.parseInt(o.toString());
    }

    static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }

        return Long.parseLong(o.toString());
    }

    static Boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }

        switch (o.toString().toUpperCase()) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }

    static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the float type.",
                                value));
            }
        }

        return Float.parseFloat(o.toString());
    }

    static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }

        return Double.parseDouble(o.toString());
    }

    public static class StructuredOptionsSplitter {

        /**
         * Splits the given string on the given delimiter. It supports quoting parts of the string with
         * either single (') or double quotes ("). Quotes can be escaped by doubling the quotes.
         *
         * <p>Examples:
         *
         * <ul>
         *   <li>'A;B';C => [A;B], [C]
         *   <li>"AB'D";B;C => [AB'D], [B], [C]
         *   <li>"AB'""D;B";C => [AB'\"D;B], [C]
         * </ul>
         *
         * <p>For more examples check the tests.
         *
         * @param string a string to split
         * @param delimiter delimiter to split on
         * @return a list of splits
         */
        static List<String> splitEscaped(String string, char delimiter) {
            List<Token> tokens = tokenize(Preconditions.checkNotNull(string), delimiter);
            return processTokens(tokens);
        }

        /**
         * Escapes the given string with single quotes, if the input string contains a double quote or
         * any of the given {@code charsToEscape}. Any single quotes in the input string will be escaped
         * by doubling.
         *
         * <p>Given that the escapeChar is (;)
         *
         * <p>Examples:
         *
         * <ul>
         *   <li>A,B,C,D => A,B,C,D
         *   <li>A'B'C'D => 'A''B''C''D'
         *   <li>A;BCD => 'A;BCD'
         *   <li>AB"C"D => 'AB"C"D'
         *   <li>AB'"D:B => 'AB''"D:B'
         * </ul>
         *
         * @param string a string which needs to be escaped
         * @param charsToEscape escape chars for the escape conditions
         * @return escaped string by single quote
         */
        static String escapeWithSingleQuote(String string, String... charsToEscape) {
            boolean escape =
                    Arrays.stream(charsToEscape).anyMatch(string::contains)
                            || string.contains("\"")
                            || string.contains("'");

            if (escape) {
                return "'" + string.replaceAll("'", "''") + "'";
            }

            return string;
        }

        private static List<String> processTokens(List<Token> tokens) {
            final List<String> splits = new ArrayList<>();
            for (int i = 0; i < tokens.size(); i++) {
                Token token = tokens.get(i);
                switch (token.getTokenType()) {
                    case DOUBLE_QUOTED:
                    case SINGLE_QUOTED:
                        if (i + 1 < tokens.size()
                                && tokens.get(i + 1).getTokenType() != TokenType.DELIMITER) {
                            int illegalPosition = tokens.get(i + 1).getPosition() - 1;
                            throw new IllegalArgumentException(
                                    "Could not split string. Illegal quoting at position: "
                                            + illegalPosition);
                        }
                        splits.add(token.getString());
                        break;
                    case UNQUOTED:
                        splits.add(token.getString());
                        break;
                    case DELIMITER:
                        if (i + 1 < tokens.size()
                                && tokens.get(i + 1).getTokenType() == TokenType.DELIMITER) {
                            splits.add("");
                        }
                        break;
                }
            }

            return splits;
        }

        private static List<Token> tokenize(String string, char delimiter) {
            final List<Token> tokens = new ArrayList<>();
            final StringBuilder builder = new StringBuilder();
            for (int cursor = 0; cursor < string.length(); ) {
                final char c = string.charAt(cursor);

                int nextChar = cursor + 1;
                if (c == '\'') {
                    nextChar = consumeInQuotes(string, '\'', cursor, builder);
                    tokens.add(new Token(TokenType.SINGLE_QUOTED, builder.toString(), cursor));
                } else if (c == '"') {
                    nextChar = consumeInQuotes(string, '"', cursor, builder);
                    tokens.add(new Token(TokenType.DOUBLE_QUOTED, builder.toString(), cursor));
                } else if (c == delimiter) {
                    tokens.add(new Token(TokenType.DELIMITER, String.valueOf(c), cursor));
                } else if (!Character.isWhitespace(c)) {
                    nextChar = consumeUnquoted(string, delimiter, cursor, builder);
                    tokens.add(new Token(TokenType.UNQUOTED, builder.toString().trim(), cursor));
                }
                builder.setLength(0);
                cursor = nextChar;
            }

            return tokens;
        }

        private static int consumeInQuotes(
                String string, char quote, int cursor, StringBuilder builder) {
            for (int i = cursor + 1; i < string.length(); i++) {
                char c = string.charAt(i);
                if (c == quote) {
                    if (i + 1 < string.length() && string.charAt(i + 1) == quote) {
                        builder.append(c);
                        i += 1;
                    } else {
                        return i + 1;
                    }
                } else {
                    builder.append(c);
                }
            }

            throw new IllegalArgumentException(
                    "Could not split string. Quoting was not closed properly.");
        }

        private static int consumeUnquoted(
                String string, char delimiter, int cursor, StringBuilder builder) {
            int i;
            for (i = cursor; i < string.length(); i++) {
                char c = string.charAt(i);
                if (c == delimiter) {
                    return i;
                }

                builder.append(c);
            }

            return i;
        }

        private enum TokenType {
            DOUBLE_QUOTED,
            SINGLE_QUOTED,
            UNQUOTED,
            DELIMITER
        }

        private static class Token {
            private final TokenType tokenType;
            private final String string;
            private final int position;

            private Token(TokenType tokenType, String string, int position) {
                this.tokenType = tokenType;
                this.string = string;
                this.position = position;
            }

            public TokenType getTokenType() {
                return tokenType;
            }

            public String getString() {
                return string;
            }

            public int getPosition() {
                return position;
            }
        }

        private StructuredOptionsSplitter() {}
    }
}
