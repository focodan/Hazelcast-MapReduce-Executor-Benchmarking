
package hazel.executor;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiMap;
import hazel.executor.HRUavgMax;
import hazel.hru.HRU;
/**
 *
 * @author daniel.elliott
 */
public class ExecutorDriver {
    private final int NUM_HRU; // how many HRUs we'll use
    private final int SLOPE_GRANULARITY; // how many divisions we'll make between [0,90]
    private final int MIN_CLUSTER_SIZE; // how many nodes must be in our cluster
    
    public static void main(String[] args){
        ExecutorDriver d = new ExecutorDriver();
        try{
            d.execute();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
    
    public ExecutorDriver(){
        this(3,100,1);
    }
    
    public ExecutorDriver(int keys, int entries, int clusterSize ){
        NUM_HRU = entries;
        SLOPE_GRANULARITY = keys;
        MIN_CLUSTER_SIZE = clusterSize;
    }
    
    //TODO add timing
    public Long execute() throws Exception {

        Config cfg = new Config();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        IExecutorService es = hz.getExecutorService("default");
        try {
            fillMapWithData(hz);

            es.submitToKeyOwner(new HRUavgMax("LOW"), "LOW", buildCallback());
            es.submitToKeyOwner(new HRUavgMax("MED"), "MED", buildCallback());
            es.submitToKeyOwner(new HRUavgMax("HIGH"), "HIGH", buildCallback());
        } finally {
            Hazelcast.shutdownAll();
        }
        return 0L;
    }
        
        private void fillMapWithData(HazelcastInstance hazelcastInstance) throws Exception {
        MultiMap<String, HRU> map = hazelcastInstance.getMultiMap("HRUs");
        for (int i = 1; i <= NUM_HRU; i++) {
            HRU tmp = new HRU();
            tmp.ID = i;
            tmp.slope = (Math.random() * (90)); // generate in range [0,90]
            String level;

            if (tmp.slope <= 30) {
                level = "LOW";
            } else if (tmp.slope <= 60) {
                level = "MED";
            } else {
                level = "HIGH";
            }
            map.put(level, tmp);
        }
    }
        
        private ExecutionCallback<String[]> buildCallback() {
        return new ExecutionCallback<String[]>() {
            @Override
            public void onResponse(String[] res) {
                System.out.println(res[0]+" "+res[1]+" "+res[2]);
            }

            @Override
            public void onFailure(Throwable throwable) {
                throwable.printStackTrace();
            }
        };
    }
}
