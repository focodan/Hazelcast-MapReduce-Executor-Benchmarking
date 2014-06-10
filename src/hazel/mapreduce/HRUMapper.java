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

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import hazel.datatypes.HRU;

public class HRUMapper
        implements Mapper<Integer, HRU, String, Integer> {
    
    private final int NUM_KEYS;
    private final int NUM_ENTRIES;

    public HRUMapper(int numKeys, int numEntries){
        NUM_KEYS = numKeys;
        NUM_ENTRIES = numEntries;
    }
    
    @Override
    public void map(Integer key, HRU hru, Context<String, Integer> context) {
        fibRecursive(31);
        context.emit(toReducerKey(hru.ID).toString(), hru.ID); //TODO refactor to Integer
    }
    
    private Integer toReducerKey(int id){
        int returnKey = id/(NUM_ENTRIES/NUM_KEYS);
        if(returnKey >= NUM_KEYS){ returnKey = NUM_KEYS-1; }
        return returnKey;
    }
    
    private long fibRecursive(int n) {
        if (n == 0 || n == 1) return n;
        else return fibRecursive(n-1) + fibRecursive(n-2);
    }
}