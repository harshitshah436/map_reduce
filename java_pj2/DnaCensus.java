//******************************************************************************
//
// File:    DnaCensus.java
// Unit:    Class edu.rit.pjmr.example.DnaCensus
//
// This Java source file is part of Programming Project 4, Map-reduce parallel 
// program using the Parallel Java Map Reduce (PJMR) framework in the Parallel
// Java 2 Library and running on a cluster parallel computer to learn about big
// data and map-reduce parallel programming.
//
//******************************************************************************

import edu.rit.pj2.vbl.IntVbl;
import edu.rit.pjmr.Combiner;
import edu.rit.pjmr.Customizer;
import edu.rit.pjmr.Mapper;
import edu.rit.pjmr.PjmrJob;
import edu.rit.pjmr.Reducer;
import edu.rit.pjmr.TextDirectorySource;
import edu.rit.pjmr.TextId;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class DnaCensus is the main program for a PJMR map-reduce job that conduct a
 * DNA census of the RefSeqGene dataset.
 * <P>
 * Usage:
 * <TT>java pj2 jar=<jar> DnaCensus <query> <directory> <node> [<node> ...] </TT>
 * 
 * <P>
 * <jar> is the name of the JAR file containing all of the program's Java class
 * files. <query> is the query sequence. <directory> is the name of the
 * directory on each node's local hard disk containing the data files to be
 * analyzed. <node> is the name of a cluster node on which to run the analysis.
 * One or more node names must be specified.
 * 
 * <P>
 * The program runs a separate mapper task on each of the given nodes. Each
 * mapper task has one source and one mapper. The source reads all files in the
 * given <directory> on the node where the mapper task is running.
 * 
 * @author Harshit
 * @version 20-Nov-2015
 */
public class DnaCensus extends PjmrJob<TextId, String, String, IntVbl> {

    /**
     * PJMR job main program.
     *
     * @param args
     *            Command line arguments.
     */
    public void main(String[] args) {
        // Parse command line arguments.
        if (args.length < 3)
            usage();
        String query = args[0];
        String directory = args[1];
        String[] nodes = new String[args.length - 2];
        for (int i = 0; i < args.length - 2; i++) {
            nodes[i] = args[i + 2];
        }

        if (args.length >= 3) {
            Pattern.compile(query); // Verify that pattern compiles
        }

        // Configure mapper tasks.
        for (String node : nodes)
            mapperTask(node).source(new TextDirectorySource(directory)).mapper(
                    MyMapper.class, query);

        // Configure reducer task.
        reducerTask().customizer(MyCustomizer.class).reducer(MyReducer.class);

        // Start this PJMR Job.
        startJob();
    }

    /**
     * Print a usage message and exit.
     */
    private static void usage() {
        System.err
                .println("Usage: java pj2 jar=<jar> DnaCensus <query> <directory> <node> [<node> ...]");
        System.err.println("<query>: is the query sequence.");
        System.err
                .println("<directory>: is the name of the directory on each node's local hard disk containing the data files to be analyzed.");
        System.err
                .println("<node>: is the name of a cluster node on which to run the analysis. One or more node names must be specified.");
        throw new IllegalArgumentException();
    }

    /**
     * Mapper class.
     */
    private static class MyMapper extends
            Mapper<TextId, String, String, IntVbl> {

        private static final IntVbl ONE = new IntVbl.Sum(1);

        // Instance variables for this mapper
        private Pattern pattern;
        private String currentDNASeqId;
        private StringBuilder currentDNASeq;

        /**
         * Start this mapper.
         */
        public void start(String[] args, Combiner<String, IntVbl> combiner) {
            if (args[0] != null) {
                pattern = Pattern.compile(args[0]);
            }
            currentDNASeq = new StringBuilder();
        }

        /**
         * Map the given input (key, value) pair to output (key, value) pair and
         * add it to a thread-local combiner.
         */
        public void map(TextId inKey, // File name and line number
                String inValue, // Line from file
                Combiner<String, IntVbl> combiner) {

            // If new DNA sequence number then calculating query occurrences
            // from previous DNA sequence, add (K,V) pairs to combiner and
            // finally setting current sequence number to new sequence number of
            // matched pattern.
            if (inValue.charAt(0) == '>') {
                countQueryOccurancesAndAddToCombiner(combiner);
                currentDNASeqId = inValue.substring(1, 30);
                currentDNASeq.delete(0, currentDNASeq.length());
            } else {
                currentDNASeq.append(inValue);
            }
        }

        /**
         * Finish this mapper.
         */
        public void finish(Combiner<String, IntVbl> combiner) {
            // Add (key,value) from the last DNA sequence to the combiner.
            countQueryOccurancesAndAddToCombiner(combiner);
        }

        /**
         * Count query occurrences from current DNA sequence and add it to
         * combiner with the current DNA sequence id.
         * 
         * @param combiner
         *            thread-local combiner.
         */
        private void countQueryOccurancesAndAddToCombiner(
                Combiner<String, IntVbl> combiner) {
            if (currentDNASeq.length() > 0) {
                Matcher matcher = pattern.matcher(currentDNASeq);
                int start_index = 0;
                while (matcher.find(start_index)) {
                    start_index = matcher.start() + 1;
                    combiner.add(currentDNASeqId, ONE);
                }
            }
        }
    }

    /**
     * Reducer task customizer class.
     */
    private static class MyCustomizer extends Customizer<String, IntVbl> {
        /**
         * Determine if the first (key, value) pair comes before the second
         * (key, value) pair in the desired sorted order.
         */
        public boolean comesBefore(String key_1, // DNA sequence ID
                IntVbl value_1, // Number of occurrences of query
                String key_2, IntVbl value_2) {
            if (value_1.item > value_2.item)
                return true;
            else if (value_1.item < value_2.item)
                return false;
            else
                return key_1.compareTo(key_2) < 0;
        }
    }

    /**
     * Reducer class.
     */
    private static class MyReducer extends Reducer<String, IntVbl> {
        /**
         * Reduce the given (key, value) pair.
         */
        public void reduce(String key, // DNA sequence ID
                IntVbl value) // Number of occurrences of query
        {
            System.out.println(key + "\t" + value);
            System.out.flush();
        }
    }

}