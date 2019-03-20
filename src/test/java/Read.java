//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.SQLContext;
//
//import java.util.Properties;
//
//public class Read {
//
//    private static void main(String [] args) {
//
//        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"));
//        SQLContext sqlContext = new SQLContext(sparkContext);
//
//        String url = "jdbc:mysql://localhost:3306/spark";
//        //查找的表名
//        String table = "news";
//        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
//        Properties connectionProperties = new Properties();
//        connectionProperties.put("user", "root");
//        connectionProperties.put("password", "root");
//        connectionProperties.put("driver", "com.mysql.jdbc.Driver");
//
//        //SparkJdbc读取Postgresql的products表内容
//        System.out.println("读取test数据库中的user_test表内容");
//
//        // 读取表中所有数据
//        sqlContext.read().jdbc(url, table, connectionProperties).createOrReplaceTempView("news");
//        Dataset jd = sqlContext.sql("SELECT * FROM t_book  ");
//        //显示数据
//        jd.show();
//
//    }
//}
