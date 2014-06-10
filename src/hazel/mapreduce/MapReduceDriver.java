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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazel.datatypes.HRUFactory;
import hazel.datatypes.HRU;
import hazel.node.UniversalHZ;
import java.util.Map;


public class MapReduceDriver {
    private final int NUM_ENTRIES; // How many test HRUs we'll generate
    private final int NUM_KEYS; // how many divisions we'll make between [0,90]
    private HazelcastInstance hz;

    public MapReduceDriver(){
        this(3,100); // default case: Run MapR job on this local instance, 100 HRUs
    }
    
    public MapReduceDriver(int keys, int entries){
        NUM_KEYS = keys;
        NUM_ENTRIES = entries;
        hz = UniversalHZ.getInstance();
    }

    // Runs a mapreduce job
    // Returns time in milliseconds for {total duration of creating data and running job, duration of running job}
    public Long[] execute() throws Exception {
        long durationTask = -1L; // How long it takes to execute job without the overhead of placing data in the cluster
        long durationTotal = -1L; // How long it takes to execute job including initializing data to place in the cluster
        Long[] durations = new Long[2];
        hazel.datatypes.HRU[] testHRUs = new hazel.datatypes.HRU[NUM_ENTRIES]; //test data
        IMap<Integer, HRU> hzMap = hz.getMap("HRUs"); 

        try {
            // get test-data in memory to avoid the creation cost in benchmark
            for(int i=0;i<NUM_ENTRIES;i++){
                testHRUs[i] = HRUFactory.getDefaultHRU();
            }

            long startTimeInitData = System.currentTimeMillis();

            fillMapWithData(testHRUs,hzMap);

            // Setup of mapreduce framework
            JobTracker jobTracker = hz.getJobTracker("default");
            KeyValueSource<Integer, HRU> source = KeyValueSource.fromMap(hzMap);
            Job<Integer, HRU> job = jobTracker.newJob(source);
            
            long startTimeExecute = System.currentTimeMillis();
            // Creating a new Job
            ICompletableFuture<Map<String, Integer[]>> future = job
                    .mapper(new HRUMapper(NUM_KEYS,NUM_ENTRIES))
                    .reducer(new HRUReducerFactory())
                    .submit();

            Map<String, Integer[]> slopeAvgs = future.get();

            long stopTime = System.currentTimeMillis();
            durationTotal = stopTime - startTimeInitData;
            durationTask = stopTime - startTimeExecute;

            // we don't need to print values for now
//            for (Map.Entry<String, Integer[]> entry : slopeAvgs.entrySet()) {
//             System.out.println("\tKey number '" + entry.getKey() + "' has entries " + entry.getValue()[0] + ",  "+entry.getValue()[1]);
//             }
        } finally {
            //Hazelcast.shutdownAll();
            (hz.getMap("HRUs")).clear();
            HRUFactory.resetIDcount();
        }
        
        durations[0] = durationTotal;
        durations[1] = durationTask;
        
        return durations;
    }

    private void fillMapWithData(HRU[] testHRUs, Map<Integer, HRU> m) throws Exception {
        Map<Integer, HRU> map = m;
        for(int i=0;i<NUM_ENTRIES;i++){
            map.put(new Integer(i), testHRUs[i]);
        }
    }

}
