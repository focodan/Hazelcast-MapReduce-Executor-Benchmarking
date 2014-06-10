/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Example modified by Dan Elliott
 */

package hazel.mapreduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class HRUReducerFactory
        implements ReducerFactory<String, Double, Double[]> {

    @Override
    public Reducer<String, Double, Double[]> newReducer(String key) {
        return new SlopeAverageMaxReducer(key);
    }

    private class SlopeAverageMaxReducer extends Reducer<String, Double, Double[]> {

        private volatile double sum = 0;
        
        private String key;

        private SlopeAverageMaxReducer(String k) {
            key = k;
        }

        @Override
        public void reduce(Double id) {
            fibRecursive(31);
            sum += id;
        }

        @Override
        public Double[] finalizeReduce() {
            fibRecursive(35);
            // Return the final reduced sum
            return new Double[] {new Double(key), sum};
        }
    }

    private long fibRecursive(int n) {
        if (n == 0 || n == 1) return n;
        else return fibRecursive(n-1) + fibRecursive(n-2);
    }
}
