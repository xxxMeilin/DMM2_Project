import java.time.ZoneId
import java.time.format.DateTimeFormatter
import sttp.client3._
import play.api.libs.json._
import java.io.{File, PrintWriter, FileOutputStream, FileNotFoundException}

import java.time.LocalDateTime
import java.time.ZonedDateTime
import scala.io.StdIn.readLine
import scala.annotation.tailrec

import scala.io.Source
import java.time.LocalDate

import scala.collection.mutable

import java.time.temporal.WeekFields
import java.util.Locale
import scala.io.Source
import scala.collection.mutable
import java.io.File

case class DataEntry(datasetId: Int, startTime: String, endTime: String, value: Double)
object project {
  val baseUrl = "https://data.fingrid.fi/api/data"
  val backend = HttpURLConnectionBackend()
  val apiKey = "774ebd90f65c4473b897ca0d893409ad"


  def main(args: Array[String]): Unit = {
    menuLoop()
  }

  @tailrec
  def menuLoop(): Unit = {
    println("Menu:")
    println("1. Data Monitor")
    println("2. Store Data")
    println("3. Create View")
    println("4. Analyse Data")
    println("5. Low Energy Output")
    println("0. Exit")
    readLine("Enter your choice: ") match {
      case "1" => dataMonitor(); menuLoop()
      case "2" => saveDataToCSV(); menuLoop()
      case "3" => predictFutureConsumption(); menuLoop()
      case "4" => checkView(); menuLoop()
      case "5" => lowEnergy(); menuLoop()
      case "0" => println("Exiting...")
      case _ => println("Invalid choice. Try again."); menuLoop()
    }
  }

  def writeDataToCSV(data: List[DataEntry], header: String, filename: String): Unit = {
    val writer = new PrintWriter(filename)
    try {
      writer.println(header)
      val dateFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
      // 反转数据列表，使得数据以倒序写入文件
      data.reverse.foreach { entry =>
        val startTime = LocalDateTime.parse(entry.startTime, dateFormatter)
        val endTime = LocalDateTime.parse(entry.endTime, dateFormatter)
        val value = entry.value
        writer.println(s"${startTime.toString},${endTime.toString},$value")
      }
    } finally {
      writer.close()
    }
  }

  def lowEnergy(): Unit = {

    println("Please choose a renewable energy source (solar, wind, hydro):")
    val energyType = scala.io.StdIn.readLine().toLowerCase

    val fileName = energyType match {
      case "solar" =>
        val file = new File("Solar.csv")
        if (file.exists()) "Solar.csv" else {
          println("Solar.csv does not exist. Please choose another energy source.\n")
          return
        }
      case "wind" =>
        val file = new File("Wind.csv")
        if (file.exists()) "Wind.csv" else {
          println("Wind.csv does not exist. Please choose another energy source.\n")
          return
        }
      case "hydro" =>
        val file = new File("Hydro.csv")
        if (file.exists()) "Hydro.csv" else {
          println("Hydro.csv does not exist. Please choose another energy source.\n")
          return
        }
      case _ =>
        println("Invalid energy type selected.")
        return
    }

    val source = Source.fromFile(fileName)
    val lines = source.getLines().drop(1)
    val data = mutable.Map[LocalDate, List[Double]]()

    lines.foreach { line =>
      val cols = line.split(",")
      if (cols.length >= 3) {
        val startTime = LocalDate.parse(cols(0).substring(0, 10), DateTimeFormatter.ISO_LOCAL_DATE) // 解析日期部分
        val value = cols(2).toDouble
        data(startTime) = data.getOrElse(startTime, List()) :+ value
      }
    }
    source.close()

    // 获取最新五天的日期
    val latestDates = data.keys.toList.sorted.takeRight(5)

    // 打印表头
    println("Date\t\t\t\tEnergy Output")
    // 打印最新五天的能量输出值
    latestDates.foreach { date =>
      data(date).foreach { value =>
        println(f"$date%-20s$value%.2f")
      }
    }
  }

  def saveDataToCSV(): Unit = {
    val solar_data = fetchData(248, 10000)
    val wind_data = fetchData(245, 10000)
    val hydro_data = fetchData(191, 10000)

    val header = "startTime,endTime,value"

    writeDataToCSV(solar_data, header, "Solar.csv")
    writeDataToCSV(wind_data, header, "Wind.csv")
    writeDataToCSV(hydro_data, header, "Hydro.csv")
  }


  implicit val dataEntryReads: Reads[DataEntry] = Json.reads[DataEntry]


  def dataMonitor(): Unit = {
    val solarData = fetchData(248, 100)
    println("Solar Energy Monitor by Hour:")
    val hourlysolarData = aggregateDataByHour(solarData)
    hourlysolarData.toList.sortBy(_._1).foreach { case (hour, sum) =>
      println(s"Time: $hour, Sum of Values: $sum kWh")
    }


    val windData = fetchData(245, 100) // Assuming 245 is the dataset ID for wind energy
    println("\nWind Energy Monitor by Hour:")
    val hourlyWindData = aggregateDataByHour(windData)
    hourlyWindData.toList.sortBy(_._1).foreach { case (hour, sum) =>
      println(s"Time: $hour, Sum of Values: $sum kWh")
    }

    val hydroData = fetchData(191, 100) // Adjusted dataset ID for wind energy
    println("\nHydropower Energy Monitor by 30 Minutes:")
    val halfHourlyData = aggregateDataBy30Minutes(hydroData)
    halfHourlyData.toList.sortBy(_._1).foreach { case (time, sum) =>
      println(s"Time: $time, Sum of Values: $sum kWh")
    }
    println("")
  }


  def fetchData(datasetId: Int, pageSize: Int): List[DataEntry] = {
    val response = basicRequest
      .header("x-api-key", apiKey)
      .get(uri"$baseUrl?datasets=$datasetId&pageSize=$pageSize")
      .send(backend)

    response.body match {
      case Right(body) =>
        val json = Json.parse(body)
        (json \ "data").validate[List[DataEntry]] match {
          case JsSuccess(data, _) =>
            data
          case JsError(errors) =>
            println(s"Failed to parse data: $errors")
            List.empty
        }
      case Left(error) =>
        println(s"API limit")
        List.empty
    }
  }


  def aggregateDataByHour(dataEntries: List[DataEntry]): Map[String, Double] = {
    val groupedByHour = dataEntries.groupBy { entry =>
      val dateTime = ZonedDateTime.parse(entry.startTime).withZoneSameInstant(ZoneId.of("UTC"))
      dateTime.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH"))
    }

    val completeHours = groupedByHour.filter(_._2.size == 4) // Ensure there are exactly 4 entries per hour

    completeHours.mapValues(_.map(_.value).sum).toMap // Convert MapView to Map
  }

  def aggregateDataBy30Minutes(dataEntries: List[DataEntry]): Map[String, Double] = {
    dataEntries.groupBy { entry =>
        val dateTime = ZonedDateTime.parse(entry.startTime).withZoneSameInstant(ZoneId.of("UTC"))
        val minute = dateTime.getMinute / 30 * 30 // Determine which half of the hour the entry belongs to
        val adjustedDateTime = dateTime.withMinute(minute).withSecond(0).withNano(0) // Normalize to the start of the half-hour
        adjustedDateTime.format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm"))
      }.filter(_._2.size == 10) // There should be exactly 10 entries per 30 minutes (3 minutes apart)
      .mapValues(_.map(_.value).sum).toMap
  }

  def predictFutureConsumption(): Unit = {
    val datasetIds = "103,104"
    val response = basicRequest.header("x-api-key", apiKey).get(uri"$baseUrl?datasets=$datasetIds").send(backend)
    println("能源消耗预测: " + response.body)
  }

  def checkView(): Unit = {
    println("Please choose a renewable energy source (solar, wind, hydro):")
    val energyType = scala.io.StdIn.readLine().toLowerCase
    val fileName = energyType match {
      case "solar" =>
        val file = new File("Solar.csv")
        if (file.exists()) {
          "Solar.csv"
        } else {
          println("Solar.csv does not exist. Please choose another energy source.\n")
          return
        }
      case "wind" =>
        val file = new File("Wind.csv")
        if (file.exists()) {
          "Wind.csv"
        } else {
          println("Wind.csv does not exist. Please choose another energy source.\n")
          return
        }
      case "hydro" =>
        val file = new File("Hydro.csv")
        if (file.exists()) {
          "Hydro.csv"
        } else {
          println("Hydro.csv does not exist. Please choose another energy source.\n")
          return
        }
      case _ =>
        println("Invalid energy type selected.\n")
        return
    }

    println("Please enter the date and hour (yyyy-MM-dd HH):")
    val input = scala.io.StdIn.readLine()
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH")

    val dateTime = try {
      LocalDateTime.parse(input, dateFormatter).withMinute(0).withSecond(0)
    } catch {
      case e: java.time.format.DateTimeParseException =>
        println("Invalid input. Please enter the date and hour in the format yyyy-MM-dd HH.\n")
        return
    }
    val userInputTimeString = dateTime.toString

    // 读取文件内容
    val source = Source.fromFile(fileName)
    val lines = try source.getLines().toList finally source.close()

    val matchingLine = lines.find { line =>
      val Array(startTime, _, _) = line.split(",") // 假设文件中每行数据以逗号分隔
      startTime.startsWith(userInputTimeString)
    }

    if (matchingLine.isDefined) {
      printf("Print the storage capacity at the time you specify: \n")
      val matchingIndex = lines.indexOf(matchingLine.get)
      val nextFourLines = lines.slice(matchingIndex, matchingIndex + 4)

      nextFourLines.zipWithIndex.foreach { case (line, index) =>
        println(s"${index + 1}. $line")
      }

      println("Which line do you want to modify? Enter the line number (1-4):")
      val lineNumber = scala.io.StdIn.readInt()

      if (lineNumber >= 1 && lineNumber <= 4) {
        println("Enter the new value:")
        val newValue = scala.io.StdIn.readDouble()

        // 修改内存中的值
        val updatedLineIndex = matchingIndex + lineNumber - 1
        val updatedLines = lines.updated(updatedLineIndex, {
          val parts = lines(updatedLineIndex).split(",")
          if (parts.length >= 3) {
            // Assuming parts(0) is start time, parts(1) is end time, and parts(2) is the value
            s"${parts(0)},${parts(1)},$newValue"
          } else {
            // 如果原始数据格式不正确，则保持原样
            lines(updatedLineIndex)
          }
        })

        try {
          val writer = new PrintWriter(new FileOutputStream(new File(fileName), false))
          try {
            updatedLines.foreach(writer.println)
          } finally {
            writer.close()
          }
        } catch {
          case e: FileNotFoundException => println("Failed to open file for writing. Make sure it is not being used by another program.")
          case e: Exception => println("An error occurred: " + e.getMessage)
        }
        println(s"Line $lineNumber updated with value $newValue.\n")
      } else {
        println("Invalid line number.\n")
      }
    } else {
      println("No matching line found.\n")
    }

  }

}