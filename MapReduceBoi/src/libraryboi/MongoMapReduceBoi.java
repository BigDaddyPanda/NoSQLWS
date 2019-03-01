package libraryboi;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import mapreduceboi.MapReduceBoi;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MongoMapReduceBoi {

    /**
     * @param args
     */
    public static String outfilename(int n) {
        return "/home/panda_pc/NetBeansProjects/MapReduceBoi/src/mapreduceboi/output.txt" + n;
    }

    public static void main(String[] args) throws Exception {

        Mongo mongo;
        String filename = "/home/panda_pc/NetBeansProjects/MapReduceBoi/src/mapreduceboi/input.txt";
        try {
            mongo = new Mongo("127.0.0.1", 27017);
            DB db = mongo.getDB("bibliotest");
            DBCollection tenancy = db.getCollection("tenancy");
            List<DBObject> records = tenancy.find().toArray();
            try {
                FileWriter writer = new FileWriter(filename);
                for (DBObject str : records) {
//                    System.out.println(str.get("isbn"));
                    writer.write(str.get("isbn") + "\n");
                }
                writer.close();
            } catch (Exception e) {
                System.err.format("Exception occurred trying to read '%s'.", filename);
                e.printStackTrace();
            }
            Configuration conf = new Configuration();
//            System.out.println("Configuration=>Done");
            Job job = Job.getInstance(conf, "word count");
//            System.out.println("Job=>Done");
            job.setJarByClass(MapReduceBoi.class);
//            System.out.println("setJarByClass=>Done");
            job.setMapperClass(MyMapper.class);
//            System.out.println("setMapperClass=>Done");
            job.setCombinerClass(MyReducer.class);
//            System.out.println("setCombinerClass=>Done");
            job.setReducerClass(MyReducer.class);
//            System.out.println("setReducerClass=>Done");
            job.setOutputKeyClass(Text.class);
//            System.out.println("setOutputKeyClass=>Done");
            job.setOutputValueClass(IntWritable.class);
//            System.out.println("setOutputValueClass=>Done");
            FileInputFormat.addInputPath(job, new Path(filename));
//            System.out.println("addInputPath=>Done");
            String out_f = outfilename(1);
            FileOutputFormat.setOutputPath(job, new Path(out_f));

//            int i = 1;
//            while (true) {
//                try {
//                    FileOutputFormat.setOutputPath(job, new Path(out_f));
//                    break;
//                } catch (Exception e) {
//                    out_f = outfilename(i++);
//
//                }
//            }
//            System.out.println("setOutputPath=>Done");
            System.exit(job.waitForCompletion(true) ? 0 : 1);
//            System.out.println(job.waitForCompletion(true) ? "Done" : "Not Yet");

            DBCollection books = db.getCollection("books");
            List<String> mapped = new ArrayList<String>();
            try {
                BufferedReader reader = new BufferedReader(new FileReader(out_f));
                String line;
                while ((line = reader.readLine()) != null) {
                    mapped.add(line);
                }
                reader.close();
            } catch (Exception e) {
                System.err.format("Exception occurred trying to read '%s'.", out_f);
            }
            for (String v : mapped) {
                BasicDBObject updateQuery = new BasicDBObject();
                updateQuery.append("$set", new BasicDBObject("stars", v.split(":")[1]));

                BasicDBObject searchQuery = new BasicDBObject();
                searchQuery.put("_id", v.split(":")[0]);
                books.update(searchQuery, updateQuery);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
//public static void main(String[] args) {
//
//        Mongo mongo;
//
//        try {
//            mongo = new Mongo("127.0.0.1", 27017);
//            DB db = mongo.getDB("bibliotest");
//
//            DBCollection books = db.getCollection("books");
//            System.out.println(books.find().toArray());;
//            System.out.println(books.find().toArray());
//            BasicDBObject book = new BasicDBObject();
//            book.put("name", "Understanding JAVA");
//            book.put("pages", 100);
//            books.insert(book);
//
//            book = new BasicDBObject();
//            book.put("name", "Understanding JSON");
//            book.put("pages", 200);
//            books.insert(book);
//
//            book = new BasicDBObject();
//            book.put("name", "Understanding XML");
//            book.put("pages", 300);
//            books.insert(book);
//
//            book = new BasicDBObject();
//            book.put("name", "Understanding Web Services");
//            book.put("pages", 400);
//            books.insert(book);
//
//            book = new BasicDBObject();
//            book.put("name", "Understanding Axis2");
//            book.put("pages", 150);
//            books.insert(book);
//
//            String map = "function() { "
//                    + "var category; "
//                    + "if ( this.pages >= 250 ) "
//                    + "category = 'Big Books'; "
//                    + "else "
//                    + "category = 'Small Books'; "
//                    + "emit(category, {name: this.name});}";
//
//            String reduce = "function(key, values) { "
//                    + "var sum = 0; "
//                    + "values.forEach(function(doc) { "
//                    + "sum += 1; "
//                    + "}); "
//                    + "return {books: sum};} ";
//
//            MapReduceCommand cmd = new MapReduceCommand(books, map, reduce,
//                    null, MapReduceCommand.OutputType.INLINE, null);
//
//            MapReduceOutput out = books.mapReduce(cmd);
//
//            for (DBObject o : out.results()) {
//                System.out.println(o.toString());
//            }
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//}
