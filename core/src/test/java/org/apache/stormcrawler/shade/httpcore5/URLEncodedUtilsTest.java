/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.stormcrawler.shade.httpcore5;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The expected values are the output of httpcore5 5.4.3 for the same input. */
class URLEncodedUtilsTest {

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            quoteCharacter = '"',
            value = {
                "a=b;c=d&e|a=b&c=d&e|[a=b, c=d, e]",
                "a+b=c+d|a+b=c+d|[a b=c d]",
                "a=%zz&b=%4|a=%25zz&b=%254|[a=%zz, b=%4]",
                "q=caf%C3%A9|q=caf%C3%A9|[q=café]",
                "\" a = b \"|a=b|[a=b]",
                "x=1&&x=2&=3|x=1&x=2|[x=1, x=2]",
                "q=~*._- !'()|q=%7E*._-+%21%27%28%29|[q=~*._- !'()]"
            })
    void parseAndFormatAsHttpCore(String query, String formatted, String pairs) {
        List<NameValuePair> parsed = URLEncodedUtils.parse(query, StandardCharsets.UTF_8, '&', ';');
        Assertions.assertEquals(pairs, parsed.toString());
        Assertions.assertEquals(formatted, URLEncodedUtils.format(parsed, StandardCharsets.UTF_8));
    }
}
