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

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.seg.common.Term;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class SegmenterUtils {

    private static final Pattern CHINESE_UNICODE_REGEX = Pattern.compile(".*[\u4e00-\u9fa5]+.*");

    private static final String SPACE = " ";

    static {
        loadUserDictionary();
    }

    private static void loadUserDictionary() {
        // TODO it can be set to read dictionary files for loading.
        addWord("us-east-1", "nz 10");
        addWord("ue1", "nz 10");
    }

    /**
     * Via delimiter segment contents.
     *
     * @param contents
     * @param delimiter
     * @return
     */
    public static List<String> simpleSegment(String contents, String delimiter) {
        if (StringUtils.isNotEmpty(contents)) {
            return new ArrayList<>(Arrays.asList(contents.split(delimiter)));
        }
        return new ArrayList<>();
    }

    /**
     * Via hanLP segment contents.
     * @param contents
     * @return
     */
    public static List<String> hanLPSegment(String contents) {
        if (StringUtils.isNotEmpty(contents)) {
            if (needSegment(contents)) {
                List<Term> termList = HanLP.segment(contents);
                return termList.stream().map(x -> x.word).filter(x -> x.trim().length() > 0).collect(Collectors.toList());
            }
            return new ArrayList<>(Collections.singleton(contents));
        }
        return new ArrayList<>();
    }

    public static boolean needSegment(String contents) {
        if (contents == null) {
            return false;
        }
        return contents.contains(SPACE) || containsChineseCharacters(contents);
    }

    public static void addWord(String word, String nature) {
        if (!StringUtils.isBlank(word) && !"null".equals(word)) {
            //Chinese and English brackets problem
            CustomDictionary.insert(word, nature);
        }
    }

    public static boolean containsChineseCharacters(String str) {
        if (str == null) {
            return false;
        }
        return CHINESE_UNICODE_REGEX.matcher(str).matches();
    }


}
