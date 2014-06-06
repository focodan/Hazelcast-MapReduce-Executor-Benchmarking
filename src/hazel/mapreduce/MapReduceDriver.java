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

//import com.hazelcast.config.Config;
//import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazel.hru.HRU;
import hazel.node.UniversalHZ;
import java.util.Map;


public class MapReduceDriver {
    private final int NUM_ENTRIES; // How many test HRUs we'll generate
    private final int NUM_KEYS; // how many divisions we'll make between [0,90]

    public MapReduceDriver(){
        this(3,100); // default case: Run MapR job on this local instance, 100 HRUs
    }
    
    public MapReduceDriver(int keys, int entries){
        NUM_KEYS = keys;
        NUM_ENTRIES = entries; 
    }

    // Runs a mapreduce job
    // Returns time in milliseconds for {total duration of creating data and running job, duration of running job}
    public Long[] execute() throws Exception {
        long durationTask = -1L; // How long it takes to execute job without the overhead of placing data in the cluster
        long durationTotal = -1L; // How long it takes to execute job including initializing data to place in the cluster
        Long[] durations = new Long[2];

        HazelcastInstance hazelcastInstance = UniversalHZ.getInstance();
        
        try {
            long startTimeInitData = System.currentTimeMillis();
            
            fillMapWithData(hazelcastInstance);
            
            long startTimeExecute = System.currentTimeMillis();
            
            Map<String, Double[]> slopeAvgs = mapReduce(hazelcastInstance);

            long stopTime = System.currentTimeMillis();
            durationTotal = stopTime - startTimeInitData;
            durationTask = stopTime - startTimeExecute;

            // we don't need to print values for now
            /*for (Map.Entry<String, Double[]> entry : slopeAvgs.entrySet()) {
                System.out.println("\tSlope type'" + entry.getKey() + "' has average " + entry.getValue()[0] + " angle, and max of "+entry.getValue()[1]);
            }*/
        } finally {
            //Hazelcast.shutdownAll();
            (hazelcastInstance.getMap("articles")).clear();
        }
        
        durations[0] = durationTotal;
        durations[1] = durationTask;
        
        return durations;
    }

    private  Map<String, Double[]> mapReduce(HazelcastInstance hazelcastInstance) throws Exception {

        // Retrieving the JobTracker by name
        JobTracker jobTracker = hazelcastInstance.getJobTracker("default");

        // Creating the KeyValueSource for a Hazelcast IMap
        IMap<Integer, HRU> map = hazelcastInstance.getMap("articles");
        KeyValueSource<Integer, HRU> source = KeyValueSource.fromMap(map);

        Job<Integer, HRU> job = jobTracker.newJob(source);

        // Creating a new Job
        ICompletableFuture<Map<String, Double[]>> future = job
                .mapper(new HRUMapper(NUM_KEYS))
                .reducer(new HRUReducerFactory())
                .submit();

        return future.get();
    }

    private  void fillMapWithData(HazelcastInstance hazelcastInstance) throws Exception {
        IMap<Integer, HRU> map = hazelcastInstance.getMap("articles");
        for(int i=1;i<=NUM_ENTRIES;i++){
            HRU tmp = new HRU();
            tmp.ID = i;
            tmp.slope = (Math.random() * (90)); // generate in range [0,90]
            map.put(new Integer(i), tmp);
        }
    }

}
