
package hazel.benchmark;

import com.hazelcast.core.Hazelcast;
import hazel.util.ResultWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author daniel.elliott
 */
public class BenchmarkTestCase {

    private Benchmark benchmark;
    
    //independent variable configuration
    private String independentVar; // This can be any of the following values:
                                   // "key", "entry"
    private int maxValue; // The largest our independent variable is allowed be
    private int step; //The value we increase our independent variable by each run
    private boolean isArithmetic; // if true, we increment independentVar by step each time.
                                  // if false, we multiply independentVar by step each time.
    private final String header; // for output to csv file

    public BenchmarkTestCase(int numKeys,int numEntries,
            String taskType,String independentVar,int maxValue,int step,boolean isArithmetic){
        
        benchmark = new Benchmark(numKeys,numEntries,taskType);
        
        this.independentVar = independentVar;
        this.maxValue = maxValue;
        this.step = step;
        this.isArithmetic = isArithmetic;
        
        header = toHeader(numKeys,numEntries,taskType,independentVar);
    }
    
    //sets step to 1, arithmetic increase of independentVar
     public BenchmarkTestCase(int numKeys,int numEntries,
            String taskType,String independentVar,int maxValue){
         this(numKeys,numEntries, taskType,independentVar, maxValue, 1, true);

    }
    
    //default test case
    public BenchmarkTestCase(){
        this(3,100,"HRU","entry",1000,100,true);
    }
    
    private int getIndepVar(){
        if(independentVar.equals("key")){
            return benchmark.getNumKeys();
        }
        else if(independentVar.equals("entry")){
            return benchmark.getNumEntries();
        }
        else{
            return -1;
        }
    }
    
    private void updateIndepVar(){
        int newVal;
        if(isArithmetic){
            newVal = getIndepVar() + step;
        }
        else{
            newVal = getIndepVar() * step;
        }
        
        if(independentVar.equals("key")){
            benchmark.updateNumKeys(newVal);
        }
        else if(independentVar.equals("entry")){
            benchmark.updateNumEntries(newVal);
        }

    }
    
    //returns list of arrays in the form: <variable value, mapreduce total time, mapreduce job time, executor total time, executor job time>
    public List<Long[]> execute(){
        List results = new ArrayList<Long[]>();
        
        while(getIndepVar() <= maxValue){
            Long[] times = benchmark.execute();
            if(times != null){
                results.add(new Long[]{(long)getIndepVar(),times[0],times[1],times[2],times[3]});
            }
            updateIndepVar();
        }
        
        return results;
    }
 
    // This runs the current configuration n number of times and does not change the value of the independent variable
    //returns a list of arrays in the form: <variable value, mapreduce total time, mapreduce job time, executor total time, executor job time>
    public List<Long[]> executeFlat(int n){
        List results = new ArrayList<Long[]>();

        for(int i=0;i<n;i++){
            Long[] times = benchmark.execute();
            if(times != null){
                results.add(new Long[]{(long)getIndepVar(),times[0],times[1],times[2],times[3]});
            }
        }

        return results;
    }
    
    // to use for CSV files we wish to write
    private static String toHeader(int numKeys,int numEntries,String taskType,String independentVar){
        return new String(
                // configuration information
                "Configuration for test case \n"+
                "#keys,#entries,task type,independent variable\n"+
                numKeys+","+numEntries+","+taskType+","+independentVar+"\n"+
                // column labels
                independentVar+",mapreduce total time,mapreduce job time,executor total time,executor job time");
    }
    
    public String getHeader(){
        return header;
    }
    
    // For now, just a simple case
    public static void main(String[] args) {
        // default, no writing to file
        /*BenchmarkTestCase b = new BenchmarkTestCase();
        List<Long[]> l = b.execute();
        System.out.println("BenchmarkTestCase default run:");
        for(Long[] res : l){
            System.out.println(res[0]+","+res[1]+","+res[2]+","+res[3]+","+res[4]);
        }*/
        
        // create a test case, run it, output results to CSV
        BenchmarkTestCase b = new BenchmarkTestCase(3,100,"HRU","entry",2000,100,true);
        String header = b.getHeader(); //toHeader(3,100,1,"HRU","entry");
        List<Long[]> results = b.executeFlat(4);
        ResultWriter w = new ResultWriter();
        w.setHeader(header);
        w.setData(results);
        w.write("testCSVSingletonFlatttat.csv");
        
        // make sure all of the instances on this JVM close.
        Hazelcast.shutdownAll();
        
    }
}
