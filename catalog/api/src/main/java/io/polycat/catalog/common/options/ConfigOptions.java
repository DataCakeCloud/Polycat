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
package io.polycat.catalog.common.options;

import java.time.Duration;
import java.util.Map;

import static io.polycat.catalog.common.utils.Preconditions.checkNotNull;

/**
 * {@code ConfigOptions} are used to build a {@link ConfigOption}. The option is typically built in
 * one of the following pattern:
 *
 * <pre>{@code
 * // simple string-valued option with a default value
 * ConfigOption<String> tempDirs = ConfigOptions
 *     .key("tmp.dir")
 *     .stringType()
 *     .defaultValue("/tmp");
 *
 * // simple integer-valued option with a default value
 * ConfigOption<Integer> parallelism = ConfigOptions
 *     .key("application.parallelism")
 *     .intType()
 *     .defaultValue(100);
 * }</pre>
 *
 *  <p>This class is modelled after apache Paimon's FallbackKey class, and partly takes code from
 *  that class. We add this code to the polycat code base in order to reduce external dependencies.
 */
public class ConfigOptions {

    /**
     * Starts building a new {@link ConfigOption}.
     *
     * @param key The key for the config option.
     * @return The builder for the config option with the given key.
     */
    public static OptionBuilder key(String key) {
        checkNotNull(key);
        return new OptionBuilder(key);
    }

    // ------------------------------------------------------------------------

    /**
     * The option builder is used to create a {@link ConfigOption}. It is instantiated via {@link
     * ConfigOptions#key(String)}.
     */
    public static final class OptionBuilder {
        /**
         * Workaround to reuse the {@link TypedConfigOptionBuilder} for a {@link Map Map&lt;String,
         * String&gt;}.
         */
        @SuppressWarnings("unchecked")
        private static final Class<Map<String, String>> PROPERTIES_MAP_CLASS =
                (Class<Map<String, String>>) (Class<?>) Map.class;

        /** The key for the config option. */
        private final String key;

        /**
         * Creates a new OptionBuilder.
         *
         * @param key The key for the config option
         */
        OptionBuilder(String key) {
            this.key = key;
        }

        /** Defines that the value of the option should be of {@link Boolean} type. */
        public TypedConfigOptionBuilder<Boolean> booleanType() {
            return new TypedConfigOptionBuilder<>(key, Boolean.class);
        }

        /** Defines that the value of the option should be of {@link Integer} type. */
        public TypedConfigOptionBuilder<Integer> intType() {
            return new TypedConfigOptionBuilder<>(key, Integer.class);
        }

        /** Defines that the value of the option should be of {@link Long} type. */
        public TypedConfigOptionBuilder<Long> longType() {
            return new TypedConfigOptionBuilder<>(key, Long.class);
        }

        /** Defines that the value of the option should be of {@link Float} type. */
        public TypedConfigOptionBuilder<Float> floatType() {
            return new TypedConfigOptionBuilder<>(key, Float.class);
        }

        /** Defines that the value of the option should be of {@link Double} type. */
        public TypedConfigOptionBuilder<Double> doubleType() {
            return new TypedConfigOptionBuilder<>(key, Double.class);
        }

        /** Defines that the value of the option should be of {@link String} type. */
        public TypedConfigOptionBuilder<String> stringType() {
            return new TypedConfigOptionBuilder<>(key, String.class);
        }

        /** Defines that the value of the option should be of {@link Duration} type. */
        public TypedConfigOptionBuilder<Duration> durationType() {
            return new TypedConfigOptionBuilder<>(key, Duration.class);
        }

        /**
         * Defines that the value of the option should be of {@link Enum} type.
         *
         * @param enumClass Concrete type of the expected enum.
         */
        public <T extends Enum<T>> TypedConfigOptionBuilder<T> enumType(Class<T> enumClass) {
            return new TypedConfigOptionBuilder<>(key, enumClass);
        }

        /**
         * Defines that the value of the option should be a set of properties, which can be
         * represented as {@code Map<String, String>}.
         */
        public TypedConfigOptionBuilder<Map<String, String>> mapType() {
            return new TypedConfigOptionBuilder<>(key, PROPERTIES_MAP_CLASS);
        }

        /**
         * Creates a ConfigOption with the given default value.
         *
         * <p>This method does not accept "null". For options with no default value, choose one of
         * the {@code noDefaultValue} methods.
         *
         * @param value The default value for the config option
         * @param <T> The type of the default value.
         * @return The config option with the default value.
         * @deprecated define the type explicitly first with one of the intType(), stringType(),
         *     etc.
         */
        @Deprecated
        public <T> ConfigOption<T> defaultValue(T value) {
            checkNotNull(value);
            return new ConfigOption<>(key, value.getClass(), ConfigOption.EMPTY_DESCRIPTION, value);
        }

        /**
         * Creates a string-valued option with no default value. String-valued options are the only
         * ones that can have no default value.
         *
         * @return The created ConfigOption.
         * @deprecated define the type explicitly first with one of the intType(), stringType(),
         *     etc.
         */
        @Deprecated
        public ConfigOption<String> noDefaultValue() {
            return new ConfigOption<>(key, String.class, ConfigOption.EMPTY_DESCRIPTION, null);
        }
    }

    /**
     * Builder for {@link ConfigOption} with a defined atomic type.
     *
     * @param <T> atomic type of the option
     */
    public static class TypedConfigOptionBuilder<T> {
        private final String key;
        private final Class<T> clazz;

        TypedConfigOptionBuilder(String key, Class<T> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        /**
         * Creates a ConfigOption with the given default value.
         *
         * @param value The default value for the config option
         * @return The config option with the default value.
         */
        public ConfigOption<T> defaultValue(T value) {
            return new ConfigOption<>(key, clazz, ConfigOption.EMPTY_DESCRIPTION, value);
        }

        /**
         * Creates a ConfigOption without a default value.
         *
         * @return The config option without a default value.
         */
        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(key, clazz, "", null);
        }
    }

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private ConfigOptions() {}
}
