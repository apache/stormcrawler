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

package org.apache.stormcrawler.parse;

public interface TextExtractor {

    String INCLUDE_PARAM_NAME = "textextractor.include.pattern";
    String EXCLUDE_PARAM_NAME = "textextractor.exclude.tags";
    String NO_TEXT_PARAM_NAME = "textextractor.no.text";
    String TEXT_MAX_TEXT_PARAM_NAME = "textextractor.skip.after";

    String text(Object element);
}
