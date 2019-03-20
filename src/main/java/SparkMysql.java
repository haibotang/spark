import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import java.util.Properties;

/**
 * Created by Administrator on 2017/11/6.
 */
public class SparkMysql {
    public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkMysql.class);

    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"));
        SQLContext sqlContext = new SQLContext(sparkContext);
        //读取mysql数据
        readMySQL(sqlContext);

        //停止SparkContext
        sparkContext.stop();
    }
    private static void readMySQL(SQLContext sqlContext){
        //jdbc.url=jdbc:mysql://localhost:3306/database
        String url = "jdbc:mysql://localhost:3306/spark";
        //查找的表名
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","root");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");


        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取spark数据库中的t_book表内容");
        // 读取表中所有数据
//        Dataset dataset = sqlContext.read().jdbc(url,table,connectionProperties).select("*");
        sqlContext.read().jdbc(url, "t_book", connectionProperties).createOrReplaceTempView("t_book");
        sqlContext.read().jdbc(url, "t_book_desc", connectionProperties).createOrReplaceTempView("t_book_desc");
        Dataset dataset =  sqlContext.sql("select a.id,count(1) from t_book a join t_book_desc b on b.b_id = a.id group by a.id");
//        Dataset dataset =  sqlContext.sql("select * from t_book ");
        //显示数据
        dataset.show();

//        SparkContext.getOrCreate(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"));
    }
}