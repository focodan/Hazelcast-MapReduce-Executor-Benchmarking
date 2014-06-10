
package hazel.executor;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.MultiMap;
import hazel.datatypes.HRU;
import hazel.datatypes.HRUFactory;
import hazel.node.UniversalHZ;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
/**
 *
 * @author daniel.elliott
 */
public class ExecutorDriver {
    private final int NUM_ENTRIES; // how many HRUs we'll use
    private final int NUM_KEYS; // how many divisions we'll make between [0,90]
    private final CountDownLatch latch; // how we will wait for the jobs to complete
    private HazelcastInstance hz;
    
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
        latch = new CountDownLatch(NUM_KEYS);
        hz = UniversalHZ.getInstance();
    }
    
    // Runs a distributed executor servre job
    // Returns time in milliseconds for {total duration of creating data and running task, duration of running task}
    public Long[] execute() throws Exception {
        //result data
        long durationTask = -1L; // How long it takes to execute job without the overhead of placing data in the cluster
        long durationTotal = -1L; // How long it takes to execute job including initializing data to place in the cluster
        Long[] durations = new Long[2];
        
        //hazelcast specific tools and data
        IExecutorService es = hz.getExecutorService("default");
        MultiMap<String, HRU> hzMultiMap = hz.getMultiMap("HRUs");

        //test data
        HRU[] testHRUs = new HRU[NUM_ENTRIES];
        
        try {
            // get test-data in memory to avoid the creation cost in benchmark
            for(int i=0;i<NUM_ENTRIES;i++){
                testHRUs[i] = HRUFactory.getDefaultHRU();
            }
            
            long startTimeInitData = System.currentTimeMillis();

            fillMapWithData(testHRUs,hzMultiMap);

            long startTimeExecute = System.currentTimeMillis();

            for(int i=0;i<NUM_KEYS;i++){
                es.submitToKeyOwner(new HRUHeavyTask((new Integer(i)).toString()), (new Integer(i)).toString(), buildCallback());
            }

            latch.await(10, TimeUnit.MINUTES); // very, very high upperbound

            long stopTime = System.currentTimeMillis();
            durationTotal = stopTime - startTimeInitData;
            durationTask = stopTime - startTimeExecute;
        } finally {
            (hz.getMultiMap("HRUs")).clear();
            HRUFactory.resetIDcount();
            //Hazelcast.shutdownAll();
        }

        durations[0] = durationTotal;
        durations[1] = durationTask;

        return durations;
    }

    private void fillMapWithData(HRU[] testHRUs, MultiMap<String, HRU> m) throws Exception {
        MultiMap<String, HRU> map = m;
        int entriesPerKey = NUM_ENTRIES/NUM_KEYS;
        int index = 0;
        for (int i = 0; i < NUM_KEYS; i++) {
            if(i+1==NUM_KEYS){ entriesPerKey = NUM_ENTRIES/NUM_KEYS + NUM_ENTRIES%NUM_KEYS; }
            for (int j=0;j<entriesPerKey;j++){
                map.put((new Integer(i)).toString(), testHRUs[index]);
                ++index;
            }
        }
    }

    private  ExecutionCallback<String[]> buildCallback() {
        return new ExecutionCallback<String[]>() {
            @Override
            public void onResponse(String[] result) {
                System.out.println(result[0]+","+result[1]);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable throwable) {
                throwable.printStackTrace();
            }
        };
    }
}
