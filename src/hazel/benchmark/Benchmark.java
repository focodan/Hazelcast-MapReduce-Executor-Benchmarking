
package hazel.benchmark;

import hazel.executor.ExecutorDriver;
import hazel.mapreduce.MapReduceDriver;

/**
 *
 * @author daniel.elliott
 */
public class Benchmark {

    private int numKeys;
    private int numEntries;
    private int minClusterSize;
    private String taskType;

    public Benchmark(int keys, int entries, int clusterSize, String task) {
        numKeys = keys;
        numEntries = entries;
        minClusterSize = clusterSize;
        taskType = task;
    }

    //simply default, local configuration
    public Benchmark() {
        this(3, 1000, 1, "HRU");
    }

    public synchronized void updateConfig(int keys, int entries, int clusterSize, String task) {
        numKeys = keys;
        numEntries = entries;
        minClusterSize = clusterSize;
        taskType = task;
    }

    // performs a benchmark according to its configuration
    // returns array of {mapReduceTime, executorTime} in milliseconds
    public synchronized Long[] execute() {
        // running times in milliseconds
        Long runTimeMapR;
        Long runTimeExecutor;
        try {
            //TODO add cases for other task types
            if (taskType.equals("HRU")) {
                ExecutorDriver ex = new ExecutorDriver(numKeys, numEntries, minClusterSize);
                runTimeExecutor = ex.execute();
                
                MapReduceDriver mr = new MapReduceDriver(numKeys, numEntries, minClusterSize);
                runTimeMapR = mr.execute();
                
                return new Long[]{runTimeMapR, runTimeExecutor};
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param args the command line arguments 
     * Usage: <#keys> <#entries> <#min cluster size> <task type>
     * Example: 3 1000 1 HRU
     */
    public static void main(String[] args) {

        Benchmark bench;

        if (args.length == 4) { //TODO add error checking on args
            bench = new Benchmark(new Integer(args[0]),new Integer(args[1]),new Integer(args[2]),args[3]);
        } else {
            bench = new Benchmark(); // uses default configuration
        }

        Long[] times = bench.execute();

        if (times != null && times.length == 2) {
            System.out.println("Times to execute in milliseconds:\n"+
                    "Mapreduce: "+times[0]+"\n"+
                    "Executor: "+times[1]);
        }
        else{
            System.out.println("Error running benchmark.");
        }

    }
}
