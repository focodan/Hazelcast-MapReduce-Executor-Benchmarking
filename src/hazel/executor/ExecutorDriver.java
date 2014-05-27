
package hazel.executor;

import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiMap;
import hazel.hru.HRU;
import java.util.concurrent.Future;
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
    
    // Runs a distributed executor servre job
    // Returns time in milliseconds for {total duration of creating data and running task, duration of running task}
    public Long[] execute() throws Exception {
        long durationTask = -1L; // How long it takes to execute job without the overhead of placing data in the cluster
        long durationTotal = -1L; // How long it takes to execute job including initializing data to place in the cluster
        Long[] durations = new Long[2];

        Config cfg = new Config();
        cfg.setProperty("hazelcast.initial.min.cluster.size", (new Integer(MIN_CLUSTER_SIZE)).toString());
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        IExecutorService es = hz.getExecutorService("default");
        try {
            long startTimeInitData = System.currentTimeMillis();

            fillMapWithData(hz);

            long startTimeExecute = System.currentTimeMillis();

            Future<String[]> low = es.submitToKeyOwner(new HRUavgMax("LOW"), "LOW");
            Future<String[]> med = es.submitToKeyOwner(new HRUavgMax("MED"), "MED");
            Future<String[]> high = es.submitToKeyOwner(new HRUavgMax("HIGH"), "HIGH");

            //block to avoid returning too early
            low.get();
            med.get();
            high.get();

            long stopTime = System.currentTimeMillis();
            durationTotal = stopTime - startTimeInitData;
            durationTask = stopTime - startTimeExecute;
        } finally {
            Hazelcast.shutdownAll();
        }

        durations[0] = durationTotal;
        durations[1] = durationTask;

        return durations;
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
