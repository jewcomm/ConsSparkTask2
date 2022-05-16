import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import java.io._


object Main extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val path = "DataSets\\TenSession\\"
  val files = new File(path).listFiles.map(_.getName).toList

  files.foreach(file=>{
    val data = spark.sparkContext.textFile(path + file)
    val frst = data.first()
    val date = frst.split(' ')(1).split('_')(0) // получили дату
    println(file + ":")  // выводим название файла, требуется для дебага, потом удалить
    var qs_status : Boolean = false
    var identArr = Seq.empty[String] // сюда пишем идентификаторы поиска

    /*
    * так как результаты поиска выводятся на следующей строке после "QS"
    * то устанавливаем в true статус
    * в следующей итерации пишем идентификатор поиска в наше объединение
    * статус обратно в false
    *
    * иные строки проверяем начинаются ли они DOC_OPEN, если да, то сверяем идентификатор с теми, что есть в нашем объединении
    * */

    data.collect.foreach(line => {
      if(qs_status){
        val ident = line.split(' ')(0)
        identArr = identArr:+ ident
        qs_status = false
      }
      else if(line.indexOf("QS") == 0){
        qs_status = true
      }
      else if(line.indexOf("DOC_OPEN") == 0){
        val sets = line.split(' ')
        if(identArr.contains(sets(2))) println(sets(3))

      }
    })

    println()
    println()
  })

  spark.stop()
}