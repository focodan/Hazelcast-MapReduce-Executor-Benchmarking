/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hazel.benchmark;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author daniel.elliott
 */
public class BenchmarkTestCase {

    private Benchmark benchmark;
    
    //independent variable configuration
    private String independentVar; // This can be any of the following values:
                                   // "key", "entry", "cluster"
    private int maxValue; // The largest our independent variable is allowed be
    private int step; //The value we increase our independent variable by each run
    private boolean isArithmetic; // if true, we increment independentVar by step each time.
                                  // if false, we multiply independentVar by step each time.

    public BenchmarkTestCase(int numKeys,int numEntries,int minClusterSize,
            String taskType,String independentVar,int maxValue,int step,boolean isArithmetic){
        
        benchmark = new Benchmark(numKeys,numEntries,minClusterSize,taskType);
        
        this.independentVar = independentVar;
        this.maxValue = maxValue;
        this.step = step;
        this.isArithmetic = isArithmetic;
    }
    
    //sets step to 1, arithmetic increase of independentVar
     public BenchmarkTestCase(int numKeys,int numEntries,int minClusterSize,
            String taskType,String independentVar,int maxValue){
         this(numKeys,numEntries,minClusterSize, taskType,independentVar, maxValue, 1, true);

    }
    
    //default test case
    public BenchmarkTestCase(){
        this(3,100,1,"HRU","entry",200,50,true);
    }
    
    private int getIndepVar(){
        if(independentVar.equals("key")){
            return benchmark.getNumKeys();
        }
        else if(independentVar.equals("entry")){
            return benchmark.getNumEntries();
        }
        else{ // is cluster size
            return benchmark.getMinClusterSize();
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
        else{ // is cluster size
            benchmark.updateMinClusterSize(newVal);
        }
    }
    
    public List<Long[]> execute(){
        List results = new ArrayList<Long[]>();
        
        while(getIndepVar() <= maxValue){
            results.add(benchmark.execute());
            updateIndepVar();
        }
        
        return results;
    }
    
    // For now, this just a test of the default test case
    public static void main(String[] args) {
        BenchmarkTestCase b = new BenchmarkTestCase();
        List<Long[]> l = b.execute();
        System.out.println("BenchmarkTestCase default run:");
        for(Long[] res : l){
            System.out.println(res[0]+","+res[1]+","+res[2]+","+res[3]);
        }
    }
}
