/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hazel.executor;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import hazel.datatypes.HRU;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.Callable;

/**
 *
 * @author daniel.elliott
 */
public class HRUHeavyTask implements Callable<Integer[]>, Serializable {

    private final String key;

    private HRUHeavyTask(){
        key = null;
    }

    public HRUHeavyTask(String k) {
        this.key = k;
    }

    @Override
    public Integer[] call() {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(new ClientConfig());

        MultiMap<String,HRU> hrusMap  = hz.getMultiMap("HRUs");
        Collection<HRU> hrus = hrusMap.get(key);
        hz.shutdown(); // perhaps change to terminate if we have better performance.
        
        int sumID = 0;
        for(HRU h : hrus){
            sumID += h.ID;
            fibRecursive(31); // ~ 18 ms penalty
        }
        fibRecursive(35); // ~ 80 ms penalty
        
        // TODO Consider timing the sum of the body of operations, returning time intervals.
        return new Integer[] {new Integer(key),sumID};
    }
    
    private long fibRecursive(int n) {
        if (n == 0 || n == 1) return n;
        else return fibRecursive(n-1) + fibRecursive(n-2);
    }
}
