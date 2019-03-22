import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class Mongodb {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("mongo").setMaster("local").set("spark.mongodb.input.uri",
                "mongodb://127.0.0.1:27017/test.t_book").set("spark.mongodb.input.uri",
                "mongodb://127.0.0.1:27017/test.t_book_desc");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());
        Dataset<Row> df = MongoSpark.load(jsc).toDF();
        df.createOrReplaceTempView("t_book");
        df.createOrReplaceTempView("t_book_desc");
        Dataset dataset = sqlContext.sql("select a.id,count(1) from t_book a join t_book_desc b on b.b_id = a.id " +
                "group by a.id");
        //显示数据
        dataset.show();
    }
}
