
package hazel.benchmark;

import hazel.util.ResultWriter;
import java.util.List;

/**
 * @author daniel.elliott
 */
public class BenchmarkTestSuite {

    /** 0th test cases: keys [3,10,20] and entries [100] as a FLAT test case, 5 iterations per case */
    public static void testSet0(String directory, int minClusterSize){   
        // Test case 0A
        BenchmarkTestCase _0A = new BenchmarkTestCase(3,100,minClusterSize,"HRU","entry",1000,100,true);
        String header = _0A.getHeader();
        List<Long[]> results = _0A.executeFlat(5);
        ResultWriter w_0A = new ResultWriter();
        w_0A.setHeader(header);
        w_0A.setData(results);
        w_0A.write(directory+"test_0A.csv");

        // Test case 0B
        BenchmarkTestCase _0B = new BenchmarkTestCase(10,100,minClusterSize,"HRU","entry",1000,100,true);
        String header_0B = _0B.getHeader();
        List<Long[]> results_0B = _0B.executeFlat(5);
        ResultWriter w_0B = new ResultWriter();
        w_0B.setHeader(header_0B);
        w_0B.setData(results_0B);
        w_0B.write(directory+"test_0B.csv");

        // Test case 0C
        BenchmarkTestCase _0C = new BenchmarkTestCase(20,100,minClusterSize,"HRU","entry",1000,100,true);
        String header_0C = _0C.getHeader();
        List<Long[]> results_0C = _0C.executeFlat(5);
        ResultWriter w_0C = new ResultWriter();
        w_0C.setHeader(header_0C);
        w_0C.setData(results_0C);
        w_0C.write(directory+"test_0C.csv");

    }

    /** 1st test cases: keys [3,10,20] and entries [100,200, ... 1,000] */
    public static void testSet1(String directory, int minClusterSize){   
        // Test case 1A
        BenchmarkTestCase _1A = new BenchmarkTestCase(3,100,minClusterSize,"HRU","entry",1000,100,true);
        String header = _1A.getHeader();
        List<Long[]> results = _1A.execute();
        ResultWriter w_1A = new ResultWriter();
        w_1A.setHeader(header);
        w_1A.setData(results);
        w_1A.write(directory+"test_1A.csv");

        // Test case 1B
        BenchmarkTestCase _1B = new BenchmarkTestCase(10,100,minClusterSize,"HRU","entry",1000,100,true);
        String header_1B = _1B.getHeader();
        List<Long[]> results_1B = _1B.execute();
        ResultWriter w_1B = new ResultWriter();
        w_1B.setHeader(header_1B);
        w_1B.setData(results_1B);
        w_1B.write(directory+"test_1B.csv");

        // Test case 1C
        BenchmarkTestCase _1C = new BenchmarkTestCase(20,100,minClusterSize,"HRU","entry",1000,100,true);
        String header_1C = _1C.getHeader();
        List<Long[]> results_1C = _1C.execute();
        ResultWriter w_1C = new ResultWriter();
        w_1C.setHeader(header_1C);
        w_1C.setData(results_1C);
        w_1C.write(directory+"test_1C.csv");

    }

    /** 2nd test cases: entries [1,000, 10,100, 100,000] and keys [10, 20, ... 100] */
    public static void testSet2(String directory, int minClusterSize){   
        // Test case 2A
        BenchmarkTestCase _2A = new BenchmarkTestCase(10,1000,minClusterSize,"HRU","key",100,10,true);
        String header = _2A.getHeader();
        List<Long[]> results = _2A.execute();
        ResultWriter w_2A = new ResultWriter();
        w_2A.setHeader(header);
        w_2A.setData(results);
        w_2A.write(directory+"test_2A.csv");

        // Test case 2B
        BenchmarkTestCase _2B = new BenchmarkTestCase(10,10000,minClusterSize,"HRU","key",100,10,true);
        String header_2B = _2B.getHeader();
        List<Long[]> results_2B = _2B.execute();
        ResultWriter w_2B = new ResultWriter();
        w_2B.setHeader(header_2B);
        w_2B.setData(results_2B);
        w_2B.write(directory+"test_2B.csv");

        // Test case 2C
        BenchmarkTestCase _2C = new BenchmarkTestCase(10,100000,minClusterSize,"HRU","key",100,10,true);
        String header_2C = _2C.getHeader();
        List<Long[]> results_2C = _2C.execute();
        ResultWriter w_2C = new ResultWriter();
        w_2C.setHeader(header_2C);
        w_2C.setData(results_2C);
        w_2C.write(directory+"test_2C.csv");

    }

    // Usage: <directory> <cluster size>
    // Example: run1/ 3
    public static void main(String[] args) {
        System.out.println("BenchmarkTestSuite");

        if (args.length == 2) {
            String directory = args[0];
            Integer clusterSize = new Integer(args[1]);
            
            if(!directory.endsWith("/")){
                directory = directory+"/";
            }

            System.out.println("test set 0");
            testSet1(directory, clusterSize);

            System.out.println("test set 1");
            testSet1(directory, clusterSize);

            System.out.println("test set 2");
            testSet2(directory, clusterSize);

        } else {
            System.out.println("Usage: <directory> <cluster size>");
        }

    }
}
