import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;

public class demo {

    public static void main(String[] args) throws Exception {
        String rootPath = System.getProperty("user.dir");
        String filePath = rootPath + "\\MySpark\\src\\main\\resources\\data.txt";

        SparkSession spark = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(filePath).javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(Pattern.compile(" ").split(s)).iterator());

        JavaRDD<HashMap<String, Integer>> ones = words.map(s ->{
            HashMap<String, Integer> v = new HashMap<>();
            v.put(s,1);
            return v;
        } );

        HashMap<String, Integer> counts = ones.reduce((s1,s2)->{
            for(String key:s2.keySet())
            {
                if (s1.containsKey(key))
                    s1.put(key,s1.get(key) + s2.get(key));
                else
                    s1.put(key,s2.get(key));
            }
            return s1;
        });

        for (String key : counts.keySet()) {
            System.out.println(key + ": " + counts.get(key));
        }

        spark.stop();
    }
}
