/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.filtering.regex;

import java.util.regex.Pattern;

/**
 * Filters URLs based on a file of regular expressions using the {@link java.util.regex Java Regex
 * implementation}.
 *
 * <p>Adapted from Apache Nutch 1.9
 */
public class RegexURLFilter extends RegexURLFilterBase {

    public RegexURLFilter() {
        super();
    }

    // Inherited Javadoc
    @Override
    protected RegexRule createRule(boolean sign, String regex) {
        return new Rule(sign, regex);
    }

    private class Rule extends RegexRule {

        private Pattern pattern;

        Rule(boolean sign, String regex) {
            super(sign, regex);
            pattern = Pattern.compile(regex);
        }

        @Override
        protected boolean match(String url) {
            return pattern.matcher(url).find();
        }
    }
}
