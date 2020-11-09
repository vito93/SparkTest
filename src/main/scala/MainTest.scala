import org.apache.spark.sql.SparkSession
import org.apache.spark

object MainTest
{
  def main(args: Array[String]): Any ={
    val spark = SparkSession.builder().appName("FirstSparkTest").master("local").config("dependencies", "com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre14").getOrCreate()
    val smth = spark.read.format("jdbc").option("url", "jdbc:sqlserver://195.133.147.60:1433").option("query", "select * from Journal.dbo.StudentEvent").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("user", "journal_reader").option("password", "journal_reader123456").load()
    val cnt = smth.count()

    printDelimiter(10)

    val columns = smth.columns

    columns.foreach(println)

    printDelimiter(3)

    smth.foreach(r => {
        print(s"$r ")
        print("\r\n")
    })

    printDelimiter(3)

    println(s"$cnt - кол-во строк из таблицы")

    printDelimiter(1)
  }

  def printDelimiter(howMany: Int) : Any ={
    var a = 0

    for(a <- 1 to howMany){
      println("---------------------------------------------------------------------")
    }
  }
}

