
package hazel.benchmark;

import hazel.executor.ExecutorDriver;
import hazel.mapreduce.MapReduceDriver;

/**
 * @author daniel.elliott
 */
public class Benchmark {

    private int numKeys;
    private int numEntries;
    private String taskType;

    public Benchmark(int keys, int entries, String task) {
        numKeys = keys;
        numEntries = entries;
        taskType = task;
    }

    //simply default, local configuration
    public Benchmark() {
        this(3, 1000, "HRU");
    }
 
 
    public int getNumKeys(){
        return numKeys;
    }
    public int getNumEntries(){
        return numEntries;
    }
    public void updateNumKeys(int keys){
        numKeys = keys;
    }
    public void updateNumEntries(int entries){
        numEntries = entries;
    }

    public void updateConfig(int keys, int entries, String task) {
        numKeys = keys;
        numEntries = entries;
        taskType = task;
    }

    // performs a benchmark according to its configuration
    // returns array of {mapReduceTime total, mapReduceTime job, executorTime total, executorTime job} in milliseconds
    public Long[] execute() {
        // running times in milliseconds
        Long[] runTimeMapR;
        Long[] runTimeExecutor;
        try {
            //TODO add cases for other task types
            if (taskType.equals("HRU")) {
                ExecutorDriver ex = new ExecutorDriver(numKeys, numEntries);
                runTimeExecutor = ex.execute();

                MapReduceDriver mr = new MapReduceDriver(numKeys, numEntries);
                runTimeMapR = mr.execute();
                //Thread.sleep(800); // allow clients to shutdown
                
                return new Long[]{runTimeMapR[0],runTimeMapR[1], runTimeExecutor[0],runTimeExecutor[1]};
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param args the command line arguments 
     * Usage: <#keys> <#entries> <task type>
     * Example: 3 1000 1 HRU
     */
    public static void main(String[] args) {

        Benchmark bench;

        if (args.length == 4) { //TODO add error checking on args
            bench = new Benchmark(new Integer(args[0]),new Integer(args[1]),args[2]);
        } else {
            bench = new Benchmark(); // uses default configuration
        }

        Long[] times = bench.execute();

        if (times != null && times.length == 4) {
            System.out.println("Times to execute in milliseconds:\n"+
                    "Mapreduce: (data initialization time + job time) "+times[0]+", (job time) "+times[1]+"\n"+
                    "Executor: (data initialization time + job time) "+times[2]+", (job time) "+times[3]);
        }
        else{
            System.out.println("Error running benchmark.");
        }

    }
}
