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
 */

package hazel.mapreduce;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazel.hru.HRU;

import java.util.Map;


/**
 * Example modified by Dan Elliott
 * 
 * A basic and simple MapReduce demo application for the Hazelcast MR framework.
 * The example Lorem Ipsum texts were created by this awesome generator: http://www.lipsum.com/
 *
 * For any further questions feel free
 * - to ask at the mailing list: https://groups.google.com/forum/#!forum/hazelcast
 * - read the Javadoc: http://hazelcast.org/docs/latest/javadoc/
 * - read the documentation this demo is for: http://bit.ly/1nQSxhH
 */
public class MapReduceDriver {
    private final int NUM_HRU; // How many test HRUs we'll generate
    private final int SLOPE_GRANULARITY; // how many divisions we'll make between [0,90]
    private final Integer MIN_C_SIZE; // How many instances we'll need in our cluster before executing a MapR job

    public MapReduceDriver(){
        this(3,100,1); // default case: Run MapR job on this local instance, 100 HRUs
    }
    
    public MapReduceDriver(int keys, int numHRU, int minClusterSize){
        SLOPE_GRANULARITY = keys;
        NUM_HRU = numHRU;
        MIN_C_SIZE = minClusterSize; 
    }

    // Runs a mapreduce job
    public Long execute() throws Exception {
        Long duration = -1L;
        Config config = new Config();
        
        // set a minimum number of nodes in the cluster before the mapreduce job can begin
        config.setProperty("hazelcast.initial.min.cluster.size", MIN_C_SIZE.toString());
        
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        try {
            long startTime = System.currentTimeMillis();
            
            fillMapWithData(hazelcastInstance);

            Map<String, Double[]> slopeAvgs = mapReduce(hazelcastInstance);

            long stopTime = System.currentTimeMillis();
            duration = stopTime - startTime;
            
            // we don't need to print values for now
            /*for (Map.Entry<String, Double[]> entry : slopeAvgs.entrySet()) {
                System.out.println("\tSlope type'" + entry.getKey() + "' has average " + entry.getValue()[0] + " angle, and max of "+entry.getValue()[1]);
            }*/
        } finally {
            Hazelcast.shutdownAll();
        }
        
        return duration;
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
                .mapper(new HRUMapper())
                .reducer(new HRUReducerFactory())
                .submit();

        return future.get();
    }

    private  void fillMapWithData(HazelcastInstance hazelcastInstance) throws Exception {
        IMap<Integer, HRU> map = hazelcastInstance.getMap("articles");
        for(int i=1;i<=NUM_HRU;i++){
            HRU tmp = new HRU();
            tmp.ID = i;
            tmp.slope = (Math.random() * (90)); // generate in range [0,90]
            map.put(new Integer(i), tmp);
        }
    }

}
