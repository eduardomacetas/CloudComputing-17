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
                if (entries.containsKey(my_entry)) {
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

//        int n_args = args.length - 1;
//        for(int i = 0; i < n_args; i++) FileInputFormat.addInputPath(job, new Path(args[i]));
//        FileOutputFormat.setOutputPath(job, new Path(args[n_args]));

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
