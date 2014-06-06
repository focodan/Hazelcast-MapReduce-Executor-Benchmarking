
package hazel.executor;

//import com.hazelcast.config.Config;
//import com.hazelcast.core.ExecutionCallback;
//import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiMap;
import hazel.hru.HRU;
import hazel.node.UniversalHZ;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
/**
 *
 * @author daniel.elliott
 */
public class ExecutorDriver {
    private final int NUM_ENTRIES; // how many HRUs we'll use
    private final int NUM_KEYS; // how many divisions we'll make between [0,90]
    
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
        this(3,100);
    }
    
    public ExecutorDriver(int keys, int entries){
        NUM_ENTRIES = entries;
        NUM_KEYS = keys;
    }
    
    // Runs a distributed executor servre job
    // Returns time in milliseconds for {total duration of creating data and running task, duration of running task}
    public Long[] execute() throws Exception {
        long durationTask = -1L; // How long it takes to execute job without the overhead of placing data in the cluster
        long durationTotal = -1L; // How long it takes to execute job including initializing data to place in the cluster
        Long[] durations = new Long[2];
        // we have NUM_KEYS number of keys, unless 90 doesn't divide it evenly. In that case, we have one more key.
        int numSlopeKeys = (90%NUM_KEYS == 0)? (NUM_KEYS): (NUM_KEYS+1);
        List<Future<String[]>> taskFutures = new ArrayList<>(NUM_KEYS);

        HazelcastInstance hz = UniversalHZ.getInstance();
        IExecutorService es = hz.getExecutorService("default");
        try {
            long startTimeInitData = System.currentTimeMillis();

            fillMapWithData(hz);

            long startTimeExecute = System.currentTimeMillis();
            
            for(int i=0;i<numSlopeKeys;i++){
                taskFutures.add(es.submitToKeyOwner(new HRUavgMax((new Integer(i)).toString()), (new Integer(i)).toString()));
            }
            
            for(int i=0;i<numSlopeKeys;i++){
                taskFutures.get(i).get();
                // In case we care to look at its output
                /*String[] res = taskFutures.get(i).get();
                for(String r: res){
                    System.out.print(r+" ");
                }System.out.println();
                */
            }

            long stopTime = System.currentTimeMillis();
            durationTotal = stopTime - startTimeInitData;
            durationTask = stopTime - startTimeExecute;
        } finally {
            (hz.getMultiMap("HRUs")).clear();
            //Hazelcast.shutdownAll();
        }

        durations[0] = durationTotal;
        durations[1] = durationTask;

        return durations;
    }

    private void fillMapWithData(HazelcastInstance hazelcastInstance) throws Exception {
        double sliceSize = 90.0 / NUM_KEYS;
        MultiMap<String, HRU> map = hazelcastInstance.getMultiMap("HRUs");
        for (int i = 1; i <= NUM_ENTRIES; i++) {
            HRU tmp = new HRU();
            tmp.ID = i;
            tmp.slope = (Math.random() * (90)); // generate in range [0,90]

            Integer slice = (int) (tmp.slope / sliceSize);
            
            map.put(slice.toString(), tmp);
        }
    }
}
