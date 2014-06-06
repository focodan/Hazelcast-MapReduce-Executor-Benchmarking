
package hazel.benchmark;

import com.hazelcast.core.Hazelcast;
import hazel.util.ResultWriter;
import java.util.List;

/**
 * @author daniel.elliott
 */
public class BenchmarkTestSuite {

    /** 0th test cases: keys [3,10,20] and entries [100] as a FLAT test case, 10 (or 30) iterations per case */
    public static void testSet0(String directory, boolean isLongTerm){
        int limit; // How many flat tests we'll perform
        if(isLongTerm){ // on long term, we do 30
            limit = 30;
        }
        else{ // short term, we do 10
            limit = 10;
        }

        // Test case 0A
        BenchmarkTestCase _0A = new BenchmarkTestCase(3,100,"HRU","entry",1000,100,true);
        String header = _0A.getHeader();
        List<Long[]> results = _0A.executeFlat(limit);
        ResultWriter w_0A = new ResultWriter();
        w_0A.setHeader(header);
        w_0A.setData(results);
        w_0A.write(directory+"test_0A.csv");

        // Test case 0B
        BenchmarkTestCase _0B = new BenchmarkTestCase(10,100,"HRU","entry",1000,100,true);
        String header_0B = _0B.getHeader();
        List<Long[]> results_0B = _0B.executeFlat(limit);
        ResultWriter w_0B = new ResultWriter();
        w_0B.setHeader(header_0B);
        w_0B.setData(results_0B);
        w_0B.write(directory+"test_0B.csv");

        // Test case 0C
        BenchmarkTestCase _0C = new BenchmarkTestCase(20,100,"HRU","entry",1000,100,true);
        String header_0C = _0C.getHeader();
        List<Long[]> results_0C = _0C.executeFlat(limit);
        ResultWriter w_0C = new ResultWriter();
        w_0C.setHeader(header_0C);
        w_0C.setData(results_0C);
        w_0C.write(directory+"test_0C.csv");

    }

    /** 1st test cases: keys [3,10,20] and entries [100,200, ... 1,000] */
    public static void testSet1(String directory, boolean isLongTerm){
        int limit; // the maximum value of our independent variable
        if(isLongTerm){
            limit = 3000;
        }
        else{
            limit = 1000;
        }
        // Test case 1A
        BenchmarkTestCase _1A = new BenchmarkTestCase(3,100,"HRU","entry",limit,100,true);
        String header = _1A.getHeader();
        List<Long[]> results = _1A.execute();
        ResultWriter w_1A = new ResultWriter();
        w_1A.setHeader(header);
        w_1A.setData(results);
        w_1A.write(directory+"test_1A.csv");

        // Test case 1B
        BenchmarkTestCase _1B = new BenchmarkTestCase(10,100,"HRU","entry",limit,100,true);
        String header_1B = _1B.getHeader();
        List<Long[]> results_1B = _1B.execute();
        ResultWriter w_1B = new ResultWriter();
        w_1B.setHeader(header_1B);
        w_1B.setData(results_1B);
        w_1B.write(directory+"test_1B.csv");

        // Test case 1C
        BenchmarkTestCase _1C = new BenchmarkTestCase(20,100,"HRU","entry",limit,100,true);
        String header_1C = _1C.getHeader();
        List<Long[]> results_1C = _1C.execute();
        ResultWriter w_1C = new ResultWriter();
        w_1C.setHeader(header_1C);
        w_1C.setData(results_1C);
        w_1C.write(directory+"test_1C.csv");

    }

    /** 2nd test cases: entries [1,000, 10,100, 100,000] and keys [10, 20, ... 100] */
    public static void testSet2(String directory, boolean isLongTerm){
        int limit; // the maximum value of our independent variable
        if(isLongTerm){
            limit = 300;
        }
        else{
            limit = 100;
        }
        // Test case 2A
        BenchmarkTestCase _2A = new BenchmarkTestCase(10,1000,"HRU","key",limit,10,true);
        String header = _2A.getHeader();
        List<Long[]> results = _2A.execute();
        ResultWriter w_2A = new ResultWriter();
        w_2A.setHeader(header);
        w_2A.setData(results);
        w_2A.write(directory+"test_2A.csv");

        // Test case 2B
        BenchmarkTestCase _2B = new BenchmarkTestCase(10,10000,"HRU","key",limit,10,true);
        String header_2B = _2B.getHeader();
        List<Long[]> results_2B = _2B.execute();
        ResultWriter w_2B = new ResultWriter();
        w_2B.setHeader(header_2B);
        w_2B.setData(results_2B);
        w_2B.write(directory+"test_2B.csv");

        // Test case 2C
        BenchmarkTestCase _2C = new BenchmarkTestCase(10,100000,"HRU","key",limit,10,true);
        String header_2C = _2C.getHeader();
        List<Long[]> results_2C = _2C.execute();
        ResultWriter w_2C = new ResultWriter();
        w_2C.setHeader(header_2C);
        w_2C.setData(results_2C);
        w_2C.write(directory+"test_2C.csv");

    }

    // Usages: <directory>
    //       : <directory> <long term>
    //       : <directory> <long term> <number trials>
    // <directory> is a string representing an existing directory
    // <long term> is a string, where "true" runs tests on a larger number of inputs than "false"
    //      default: false
    // <number trials> is an int representing the number of times the test set is ran.
    //      default: 1
    // Examples: run1/ 3
    //         : tmp 2 false 3
    public static void main(String[] args) {
        int numTrials = 1; // how many times should each test be repeated
        boolean isLongTerm = false; // whether to run the tests are a larger scale
        String directory = "";
        String usage = "Usages: <directory>\n"
                +"      : <directory> <long term>\n"
                +"      : <directory> <long term> <number trials>\n"
                +"<directory> is a string representing an existing directory\n"
                +"<long term> is a string, where 'true' runs tests on a larger number of inputs than 'false'\n"
                +"     default: false\n"
                +"<number trials> is an int representing the number of times the test set is ran.\n"
                +"     default: 1\n"
                +"Examples: run1/ true\n"
                +"        : tmp false 3";


        // parse command line args
        if (args.length <= 4 && args.length >= 1) {
            directory = args[0];
            if (!directory.endsWith("/")) {
                directory = directory + "/";
            }
            if (args.length >= 2) {
                String longTerm = args[1];
                if (!(longTerm.equalsIgnoreCase("true") || longTerm.equalsIgnoreCase("false"))) {
                    System.out.println(usage);
                    System.exit(0);
                }
                isLongTerm = (longTerm.equalsIgnoreCase("true"));
            }
            if (args.length >= 3) {
                numTrials = new Integer(args[2]);
            }
        }
        else { // incorrect number of arguments given
            System.out.println(usage);
            System.exit(0);
        }

        // output configuration
        System.out.println("BenchmarkTestSuite");
        System.out.println("directory ("+directory+
                "), long term ("+isLongTerm+"), number of trials("+numTrials+")");

        // run the test sets
        for(int i=0;i<numTrials;i++){
            String trialDir = directory+"trial_"+(new Integer(i)).toString()+"_";
                System.out.println("test set 0");
                testSet0(trialDir, isLongTerm);

                System.out.println("test set 1");
                testSet1(trialDir, isLongTerm);

                System.out.println("test set 2");
                testSet2(trialDir, isLongTerm);
            }
        // make sure all of the hz instances on this JVM close.
        Hazelcast.shutdownAll();
        }
    }

