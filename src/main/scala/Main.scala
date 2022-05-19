import org.apache.spark.sql.types.{DateType}
import org.apache.spark.sql.{SparkSession, functions}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

import java.io._
import java.nio.file._


object Main extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Sample App")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val path = "DataSets\\ConsSession\\" // путь к исходным логам
  val files = new File(path).listFiles.map(_.getName).toList // получаем список файлов

  var rawsDO = Seq.empty[(String, String)] // объединение для записи результата

  files.foreach(file=>{
    val raw = spark.sparkContext.textFile(path + file)
    var qsStatus : Boolean = false
    var identArr = Seq.empty[String] // сюда пишем идентификаторы поиска
    var searchData : String = ""

    /*
    * так как результаты поиска выводятся на следующей строке после "QS"
    * то устанавливаем в true статус
    * в следующей итерации пишем идентификатор поиска в наше объединение
    * статус обратно в false
    *
    * иные строки проверяем начинаются ли они DOC_OPEN, если да, то сверяем идентификатор с теми, что есть в нашем объединении
    * */

    raw.collect.foreach(line => {
      if(qsStatus){
        val ident = line.split(' ')(0)
        identArr = identArr:+ ident
        qsStatus = false
      }
      else if(line.indexOf("QS") == 0){
        qsStatus = true
        searchData = line.split(' ')(1).split('_')(0)
      }
      else if(line.indexOf("DOC_OPEN") == 0){
        val docOpenData = line.split(' ')
        var openDate = docOpenData(1).split('_')(0) // дата открытия

        if(openDate.length != 10) openDate = searchData // если дата открытия не написана в строке DOC_OPEN

        if(identArr.contains(docOpenData(2))) {
          val docIdent = docOpenData(3) // идентификатор документа

          if(docIdent.indexOf("_") == -1) println("Error with doc identificator in file: " + file)

          rawsDO = rawsDO :+ (openDate, docIdent)
        }
      }
    })
  })

  val RDD = spark.sparkContext.parallelize(rawsDO) // помещаем в RDD полученные результаты

  val df = spark.createDataFrame(RDD).toDF("Date", "Ident") // создаем датафрейм для полученных результатов

  // название папки куда кладем результат
  var pathResult : String = "result"+ DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm").format(LocalDateTime.now)

  // если папка уже существует, то добавляем по 1 к окончанию названия
  if(Files.exists(Paths.get(pathResult))){
    var i = 1
    while(Files.exists(Paths.get(pathResult + "_" +i))) i = i + 1
    pathResult = pathResult + "_" + i
  }

  (df.groupBy("Date", "Ident")
    .agg(functions.count("*"))) // группируем по дате и идентификатору, и считаем повторы
    .orderBy(functions.to_date(functions.column("Date"), "dd.MM.yyyy").cast(DateType).asc) // упорядочиваем по увеличению значения
    .write.format("csv").save(pathResult) // сохраняем результаты в папку


  // удаляем из папки с результатами файлы с расширеним .crc и файл _SUCCESS
  val filesInResultFolder = new File(pathResult).listFiles.map(_.getName).toList
  filesInResultFolder.foreach(fileName=>{
    val extension = fileName.split('.').last
    if(extension == "crc" || extension == "_SUCCESS"){
      try{
        Files.deleteIfExists(Paths.get(pathResult + "/" + fileName))
      }
      catch {
        case e: NoSuchFileException => println("No such file/directory exists ")
        case e: IOException => println("Invalid permissions")
      }
    }
  })

  spark.stop()
}