import com.mongodb.spark.MongoSpark;
import model.Book;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkMongodb {

    public static void main(String [] args){
//        SparkSession session = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
//                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test")
//                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test")
//                .getOrCreate();
//        MongoSpark.load(session);

        SparkConf conf = new SparkConf().setAppName("mongo").setMaster("local").set("spark.mongodb.input.uri","mongodb://127.0.0.1:27017/test.t_book");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());
        MongoSpark.load(jsc).toDF().createOrReplaceTempView("t_book");
        MongoSpark.load(jsc).toDF().createOrReplaceTempView("t_book_desc");
//        Dataset dataset = sqlContext.sql("select * from t_book");
        Dataset dataset = sqlContext.sql("select a.id,count(1) from t_book a join t_book_desc b on b.b_id = a.id group by a.id");
        //显示数据
        dataset.show();
    }
}
