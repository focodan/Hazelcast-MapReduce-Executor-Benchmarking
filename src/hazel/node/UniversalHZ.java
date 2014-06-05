package hazel.node;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * @author daniel.elliott
 */
// This is a shared hazelcast instance, so that I won't have to create a 
// new instance per execution of MapReduceDriver or ExecutorDriver.
public class UniversalHZ {
    private static final HazelcastInstance hz = Hazelcast.newHazelcastInstance(null);
    
    private UniversalHZ(){}
    
    public static HazelcastInstance getInstance(){
        return hz;
    }
}
