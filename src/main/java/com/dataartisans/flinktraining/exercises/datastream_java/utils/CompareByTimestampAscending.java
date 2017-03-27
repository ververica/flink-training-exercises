/*
 * Copyright 2017 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.utils;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import java.util.Comparator;

public class CompareByTimestampAscending implements Comparator<ConnectedCarEvent> {

    @Override
    public int compare (ConnectedCarEvent a, ConnectedCarEvent b) {
        if (a.timestamp > b.timestamp)
            return 1;
        if (a.timestamp == b.timestamp)
            return 0;
        return -1;
    }
}
