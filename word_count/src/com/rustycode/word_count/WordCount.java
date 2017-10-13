package com.rustycode.word_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class WordCount {
    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, Text> {

        private Text result = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            kotlin.jvm.JvmClassMappingKt.getKotlinClass(Stemmer.class);
            Stemmer stemmer = new Stemmer();

            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^A-Za-z]", " "));
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            while (itr.hasMoreTokens()) {
                word.set(stemmer.stem(itr.nextToken()));
                result.set("<"+fileName + ", 1>");
                context.write(word, result);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public LinkedHashMap<String, Integer> sortHashMapByValues(HashMap<String, Integer> passedMap) {
            List<String> mapKeys = new ArrayList<String>(passedMap.keySet());
            List<Integer> mapValues = new ArrayList<Integer>(passedMap.values());
            Collections.sort(mapValues);
            Collections.reverse(mapValues);
            Collections.sort(mapKeys);

            LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();

            Iterator valueIt = mapValues.iterator();
            while (valueIt.hasNext()) {
                Object val = valueIt.next();
                Iterator keyIt = mapKeys.iterator();

                while (keyIt.hasNext()) {
                    Object key = keyIt.next();
                    Integer comp1 = passedMap.get(key.toString());
                    Integer comp2 = (Integer)val;

                    if (comp1.equals(comp2)){
                        passedMap.remove(key.toString());
                        mapKeys.remove(key);
                        sortedMap.put(key.toString(), (Integer)val);
                        break;
                    }

                }

            }
            return sortedMap;
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String, Integer> entries = new HashMap<String, Integer>();
            String filename;

            for (Text val : values) {
                String value = val.toString();
                filename =  value.split(",")[0];
                value = value.split(", ")[1].split(">")[0];
                String my_entry = filename;
                if (entries.containsKey(my_entry)){
                    entries.put(my_entry, entries.get(my_entry) + Integer.parseInt(value));
                }
                else {
                    entries.put(my_entry, Integer.parseInt(value));
                }
            }

            HashMap<String, Integer> sorted = sortHashMapByValues(entries);
            Iterator it = sorted.entrySet().iterator();
            String my_values = "";

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                filename = pair.getKey().toString();
                if (!filename.contains("\n")){
                    filename  = "\n" + filename;
                }
                my_values += filename + ", " + pair.getValue() + ">";

                it.remove();
            }

            result.set(my_values);

            if (!key.toString().contains("\n")){
                key.set("\n" + key.toString());
            }

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//import com.rustycode.word_count.Stemmer;
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.net.URI;
//import java.util.HashSet;
//import java.util.Set;
//import java.io.IOException;
//import java.util.regex.Pattern;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.util.StringUtils;
//
//import org.apache.log4j.Logger;
//
//public class WordCount extends Configured implements Tool {
//
//    private static final Logger LOG = Logger.getLogger(WordCount.class);
//
//    public static void main(String[] args) throws Exception {
//        int res = ToolRunner.run(new WordCount(), args);
//        System.exit(res);
//    }
//
//    public int run(String[] args) throws Exception {
//        Job job = Job.getInstance(getConf(), "wordcount");
//        for (int i = 0; i < args.length; i += 1) {
//            if ("-skip".equals(args[i])) {
//                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
//                i += 1;
//                job.addCacheFile(new Path(args[i]).toUri());
//                // this demonstrates logging
//                LOG.info("Added file to the distributed cache: " + args[i]);
//            }
//        }
//        job.setJarByClass(this.getClass());
//        // Use TextInputFormat, the default unless job.setInputFormatClass is used
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        job.setMapperClass(Map.class);
//        job.setCombinerClass(Reduce.class);
//        job.setReducerClass(Reduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        return job.waitForCompletion(true) ? 0 : 1;
//    }
//
//    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//        private boolean caseSensitive = false;
//        private long numRecords = 0;
//        private String input;
//        private Set<String> patternsToSkip = new HashSet<String>();
//        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
//
//        protected void setup(Mapper.Context context)
//                throws IOException,
//                InterruptedException {
//            if (context.getInputSplit() instanceof FileSplit) {
//                this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
//            } else {
//                this.input = context.getInputSplit().toString();
//            }
//            Configuration config = context.getConfiguration();
//            this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
//            if (config.getBoolean("wordcount.skip.patterns", false)) {
//                URI[] localPaths = context.getCacheFiles();
//                parseSkipFile(localPaths[0]);
//            }
//        }
//
//        private void parseSkipFile(URI patternsURI) {
//            LOG.info("Added file to the distributed cache: " + patternsURI);
//            try {
//                BufferedReader fis = new BufferedReader(new FileReader(new File(patternsURI.getPath()).getName()));
//                String pattern;
//                while ((pattern = fis.readLine()) != null) {
//                    patternsToSkip.add(pattern);
//                }
//            } catch (IOException ioe) {
//                System.err.println("Caught exception while parsing the cached file '"
//                        + patternsURI + "' : " + StringUtils.stringifyException(ioe));
//            }
//        }
//
//        public void map(LongWritable offset, Text lineText, Context context)
//                throws IOException, InterruptedException {
//            String line = lineText.toString();
//            if (!caseSensitive) {
//                line = line.toLowerCase();
//            }
//            Text currentWord = new Text();
//
////            Stemmer stemmer = new Stemmer();
//
//            for (String word : WORD_BOUNDARY.split(line)) {
//                if (word.isEmpty() || patternsToSkip.contains(word)) {
//                    continue;
//                }
//                currentWord = new Text(word);
//                context.write(currentWord,one);
//            }
//        }
//    }
//
//    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//        @Override
//        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
//                throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable count : counts) {
//                sum += count.get();
//            }
//            context.write(word, new IntWritable(sum));
//        }
//    }
//}