//Group 17
//Group Member (alphabetical): Meilin Zhang, Wenrui Feng, Yixuan Li
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import sttp.client3._
import play.api.libs.json._
import java.io.{File, FileNotFoundException, FileOutputStream, PrintWriter}
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.ZonedDateTime
import scala.io.StdIn.readLine
import scala.annotation.tailrec
import scala.io.Source
import java.time.LocalDate
import java.time.temporal.WeekFields
import java.util.Locale
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class DataEntry(datasetId: Int, startTime: String, endTime: String, value: Double)
object project {
  val baseUrl = "https://data.fingrid.fi/api/data"
  val backend = HttpURLConnectionBackend()
  val apiKey = "1fe48e7e12e54c1791b8d719b22784ce"


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
      case "3" => checkView(); menuLoop()
      case "4" => analyzeRenewableData();menuLoop()
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
      // Reverse the data list so that the data is written to the file in reverse order
      data.reverse.foreach { entry =>
        val startTime = LocalDateTime.parse(entry.startTime, dateFormatter)
        val endTime = LocalDateTime.parse(entry.endTime, dateFormatter)
        val value = entry.value
        writer.println(s"${startTime.toString},${endTime.toString},$value")
      }
    }
    finally {
      writer.close()
    }
  }

  def lowEnergy(): Unit = {
    println("Please choose a renewable energy source (solar, wind, hydro):")
    val energyType = scala.io.StdIn.readLine().toLowerCase
    // Determine the file name based on user input
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
          println("Wind.csv does not exist. Please choose 2 to download data.\n")
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
        println("Invalid energy type selected.")
        return
    }
    // Reading CSV Files
    val source = Source.fromFile(fileName)
    val lines = Source.fromFile(fileName).getLines().drop(1) // Assume the first row is the table header
    val data = mutable.Map[LocalDateTime, List[Double]]()

    // Data Analysis
    lines.foreach { line =>
      val cols = line.split(",")
      if (cols.length >= 3) { // Ensure there are enough columns
        val startTime = LocalDateTime.parse(cols(0), DateTimeFormatter.ISO_LOCAL_DATE_TIME) // Parse the first column as the start time
        val value = cols(2).toDouble // The third column is the energy output value.
        data(startTime) = data.getOrElse(startTime, List()) :+ value // Add data to the map
      }
    }
    source.close()
    // Aggregation at the date level is daily average
    val dailyAverage = data.groupBy(_._1.toLocalDate).map { case (date, timesAndOutputs) =>
      val dailyOutputs = timesAndOutputs.flatMap(_._2)
      date -> (dailyOutputs.sum / dailyOutputs.size)
    }

    // Calculate the sliding average of the previous five days
    val sortedDates = dailyAverage.keys.toList.sorted
    val rollingAverages = sortedDates.map { date =>
      val prevDays = sortedDates.filter(d => d.isBefore(date) && d.isAfter(date.minusDays(6)))
      val avg = if (prevDays.isEmpty) 0 else prevDays.map(dailyAverage).sum / prevDays.size
      date -> avg
    }.toMap

    // The user enters the date they want to query
    println("Please enter the date you are interested in (format YYYY-MM-DD):")
    val inputDateStr = scala.io.StdIn.readLine()
    try {
      val inputDate = LocalDate.parse(inputDateStr, DateTimeFormatter.ISO_LOCAL_DATE)

      // Output the average energy output of the selected date and the average output of the previous five days, compare and issue an alarm
      dailyAverage.get(inputDate) match {
        case Some(avgEnergy) =>
          val avgFiveDays = rollingAverages.getOrElse(inputDate, 0.0)
          println(s"On $inputDate, the average energy output is ${avgEnergy}.")
          if (avgEnergy < avgFiveDays) {
            println(s"which is compared to the previous five days average of ${avgFiveDays}.")
            println("Alert: Average energy output is lower than the previous five days average.")
          } else if (avgFiveDays == 0.0) {
            println("There is not enough data to calculate the average energy output for the previous five days.")
          } else {
            println(s"which is compared to the previous five days average of ${avgFiveDays}.")
            println("Average energy output is not lower than the previous five days average.")
          }
        case None =>
          println(s"No data available for $inputDate.")
      }
    } catch {
      case e: Exception =>
        println("Invalid input. Please enter the date in the format yyyy-MM-dd.\nFor example, enter '2024-04-12' for April 12, 2024. ")
    }
  }

  def saveDataToCSV(): Unit = {
    val solar_data = fetchData(248, 10000)
    val wind_data = fetchData(245, 10000)
    val hydro_data = fetchData(191, 10000)
    val header = "startTime,endTime,value"
    checkFileExists("Solar.csv")
    writeDataToCSV(solar_data, header, "Solar.csv")
    checkFileExists("Wind.csv")
    writeDataToCSV(wind_data, header, "Wind.csv")
    checkFileExists("Hydro.csv")
    writeDataToCSV(hydro_data, header, "Hydro.csv")
  }

  def checkFileExists(filePath: String): Unit = {
    if (Files.exists(Paths.get(filePath))) {
      println(s"File $filePath already exists.")
    } else {
      println(s"Create $filePath successful")
    }
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
    // Reading file contents
    val source = Source.fromFile(fileName)
    val lines = try source.getLines().toList finally source.close()
    val matchingLine = lines.find { line =>
      val Array(startTime, _, _) = line.split(",") // Assume that each line of data in the file is separated by commas
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
        // Modify the value in memory
        val updatedLineIndex = matchingIndex + lineNumber - 1
        val updatedLines = lines.updated(updatedLineIndex, {
          val parts = lines(updatedLineIndex).split(",")
          if (parts.length >= 3) {
            // Assuming parts(0) is start time, parts(1) is end time, and parts(2) is the value
            s"${parts(0)},${parts(1)},$newValue"
          } else {
            // If the original data is not in the correct format, keep it as is
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
  def processHourlyData(startDate: LocalDateTime, filename: String): Unit = {
    var resultList: List[String] = List()
    val endDate = startDate.plusDays(1)
    val userInputStartDateTimeString = startDate.toString
    val userInputEndDateTimeString = endDate.toString
    val source = Source.fromFile(filename)
    val lines = try source.getLines().toList finally source.close()
    val (matchingStartDate, matchingEndDate) = lines.foldLeft((Option.empty[String], Option.empty[String])) { (acc, line) =>
      val Array(startTime, endTime, _) = line.split(",") // Assume that each line of data in the file is separated by commas
      val (startDateFound, endDateFound) = acc
      if (startDateFound.isEmpty && startTime.startsWith(userInputStartDateTimeString)) {
        (Some(startTime), endDateFound)
      } else if (endDateFound.isEmpty && endTime.startsWith(userInputEndDateTimeString)) {
        (startDateFound, Some(endTime))
      } else {
        acc
      }
    }

    if (matchingStartDate.isDefined && matchingEndDate.isDefined) {
      val filteredLines = lines.filter { line =>
        val parts = line.split(",")
        val startTime = parts(0)
        val endTime = parts(1)
        startTime >= matchingStartDate.get && endTime <= matchingEndDate.get
      }
      val chunkedSums = filteredLines.map { line =>
          val parts = line.split(",")
          parts(2).toDouble // Extract the numeric value of each row and convert it to Double
        }
        .grouped(4) // Group the values into groups of four
        .map(_.sum) // Sum each group
        .zipWithIndex // Generate an index for each group
        .map { case (sum, index) =>
          (f"$index%02d", sum) // Using formatted index numbers
        }
        .toList // Convert to a list
      val sums = chunkedSums.map(_._2)
      if (sums.nonEmpty) {
        // Print the sum of each group
        chunkedSums.foreach { case (index, sum) =>
          println(s"Hours $index: $sum")
        }

        // Calculate the mean
        val mean = sums.sum / sums.length
        println(s"Mean: $mean")

        // Calculate the median
        val sortedSums = sums.sorted
        val median = if (sortedSums.length % 2 != 0) {
          sortedSums(sortedSums.length / 2)
        } else {
          val mid = sortedSums.length / 2
          (sortedSums(mid - 1) + sortedSums(mid)) / 2
        }
        println(s"Median: $median")

        // Calculate the mode
        val counts = sums.groupBy(identity).mapValues(_.size)
        val maxCount = counts.values.max
        val mode = counts.filter(_._2 == maxCount).keys.toList
        println(s"Mode: ${mode.mkString(", ")}")

        // Calculate the range
        val minSum = sortedSums.head
        val maxSum = sortedSums.last
        val range = maxSum - minSum
        println(s"Range: $range")

        // Calculate the midrange
        val midrange = (minSum + maxSum) / 2
        println(s"Midrange: $midrange")
      } else {
        println("No data found.")
      }
    }
    println("\n")
  }

  def calculateStatistics(data: List[String]): Unit = {
    val values = data.map(_.split("\\s+")(1).toDouble)

    // Mean Calculation
    val mean = values.sum / values.length

    // Median Calculation
    val sortedValues = values.sorted
    val median = if (sortedValues.length % 2 != 0) {
      sortedValues(sortedValues.length / 2)
    } else {
      (sortedValues(sortedValues.length / 2 - 1) + sortedValues(sortedValues.length / 2)) / 2
    }

    // Mode Calculation
    val valueCounts = values.groupBy(identity).mapValues(_.size)
    val maxCount = valueCounts.values.max
    val mode = valueCounts.filter(_._2 == maxCount).keys.mkString(", ")

    // Range Calculation
    val range = sortedValues.last - sortedValues.head

    // Midrange Calculation
    val midrange = (sortedValues.last + sortedValues.head) / 2

    println(s"Mean: $mean")
    println(s"Median: $median")
    println(s"Mode: $mode")
    println(s"Range: $range")
    println(s"Midrange: $midrange")
  }

  def processMonthlyData(startDate: LocalDateTime, filename: String): Unit = {
    val source = Source.fromFile(filename)
    val lines = source.getLines().drop(1)
    val data = mutable.Map[LocalDateTime, Double]()

    lines.foreach { line =>
      val cols = line.split(",")
      if (cols.length >= 3) {
        val startTime = LocalDateTime.parse(cols(0), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val value = cols(2).toDouble
        data(startTime) = data.getOrElse(startTime, 0.0) + value
      }
    }
    source.close()

    val monthlyTotal = data.groupBy(_._1.toLocalDate.withDayOfMonth(1))
      .map { case (month, timesAndOutputs) =>
        month -> timesAndOutputs.map(_._2).sum
      }

    val formattedResults = ListBuffer[String]()
    println("Month\t\t\tTotal Energy Output")

    // Convert map to list, sort by month, format each entry, and add to the list
    monthlyTotal.toList.sortBy(_._1).foreach { case (month, total) =>
      formattedResults += f"$month%-20s$total%.2f"
    }

    // Return the list of formatted strings
    val resultList = formattedResults.toList
    formattedResults.foreach(println)
    calculateStatistics(resultList)
    println("\n")
  }

  def analyzeRenewableData(): Unit = {
    println("Which renewable energy source would you like to analyze?")
    println("1. Solar panel")
    println("2. Wind turbine")
    println("3. Hydropower")
    // Assume sourceChoice is a choice entered by the user from the console
    val sourceChoice = scala.io.StdIn.readInt()
    val sourceFilename = sourceChoice match {
      case 1 =>
        val file = new File("Solar.csv")
        if (file.exists()) {
          Some(file)
        } else {
          println("Solar.csv does not exist. Please choose another energy source.\n")
          None
        }
      case 2 =>
        val file = new File("Wind.csv")
        if (file.exists()) {
          Some(file)
        } else {
          println("Wind.csv does not exist. Please choose 2 to download data.\n")
          None
        }
      case 3 =>
        val file = new File("Hydro.csv")
        if (file.exists()) {
          Some(file)
        } else {
          println("Hydro.csv does not exist. Please choose another energy source.\n")
          None
        }
      case _ =>
        println("Invalid energy type selected.")
        None
    }

    sourceFilename.foreach { file =>
      println("Please enter the date (yyyy-MM-dd):")
      val input = scala.io.StdIn.readLine()
      val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

      try {
        val parsedDate = LocalDate.parse(input, dateFormatter)
        val dateTime = parsedDate.atStartOfDay()
        val userInputTimeString = dateTime.toString
        val source = Source.fromFile(file)
        val lines = try source.getLines().toList finally source.close()
        val matchingLine = lines.find { line =>
          val Array(startTime, _, _) = line.split(",") // Assume that each line of data in the file is separated by commass
          startTime.startsWith(userInputTimeString)
        }

        if (matchingLine.isDefined) {
          menuLoop(dateTime, file.getName)
        }else{
          println("The searched date does not exist in the database\n")
        }

      } catch {
        case e: java.time.format.DateTimeParseException =>
          println("Invalid input. Please enter the date in the format yyyy-MM-dd.\nFor example, enter '2024-04-12' for April 12, 2024.")
        case _: Throwable =>
          println("Error occurred while processing the file.")
      }
    }

    def menuLoop(dateTime: LocalDateTime, filename: String): Unit = {
      println("Select the data retrieval method:")
      println("1. Hourly")
      println("2. Daily")
      println("3. Weekly")
      println("4. Monthly")
      readLine("Enter your choice: ") match {
        case "1" => processHourlyData(dateTime, filename)
        case "2" => processDailyData(dateTime, filename)
        case "3" => processWeeklyData(dateTime, filename)
        case "4" => processMonthlyData(dateTime, filename)
        case _ =>
          println("Invalid choice. Try again.")
          menuLoop(dateTime, filename)
      }
    }
  }

  def processDailyData(startDate: LocalDateTime, filename: String): Unit = {
    val source = Source.fromFile(filename)
    val lines = source.getLines().drop(1)
    val data = mutable.Map[LocalDate, List[Double]]()

    lines.foreach { line =>
      val cols = line.split(",")
      if (cols.length >= 3) {
        val startTime = LocalDate.parse(cols(0).substring(0, 10), DateTimeFormatter.ISO_LOCAL_DATE) // Parsing the date part
        val value = cols(2).toDouble
        data(startTime) = data.getOrElse(startTime, List()) :+ value
      }
    }
    source.close()

    // Get the latest five days' date
    val latestDates = data.keys.toList.sorted.takeRight(5)

    // Print the head
    println("Date\t\t\t\tEnergy Output")
    // Print the energy output values for the latest five days
    // Create a mutable Map to accumulate the output values for each date
    val aggregatedData = mutable.Map[String, Double]().withDefaultValue(0.0)

    // Iterate over latestDates and add the output values with the same date
    latestDates.foreach { date =>
      data(date).foreach { value =>
        val dateString = date.toString // Convert a date to a string
        aggregatedData(dateString) += value
      }
    }
    println("Date Range\t\t\tTotal Energy Output")

    // Convert the accumulated data into the appropriate format
    val dataList = aggregatedData.map { case (date, total) =>
      s"$date\t\t$total"
    }.toList
    dataList.foreach(println)

    // Calling the calculateStatistics function
    calculateStatistics(dataList)
    println("\n")
  }

  def processWeeklyData(startDate: LocalDateTime, filename: String): Unit = {
    val source = Source.fromFile(filename)
    val lines = source.getLines().drop(1)
    val data = mutable.Map[LocalDateTime, Double]()

    lines.foreach { line =>
      val cols = line.split(",")
      if (cols.length >= 3) {
        val startTime = LocalDateTime.parse(cols(0), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val value = cols(2).toDouble
        data(startTime) = data.getOrElse(startTime, 0.0) + value
      }
    }
    source.close()

    val weekFields = WeekFields.of(Locale.getDefault)
    val weeklyTotal = data.groupBy { case (dateTime, _) =>
        val week = dateTime.toLocalDate.get(weekFields.weekOfWeekBasedYear())
        val year = dateTime.getYear
        year * 100 + week
      }
      .map { case (weekYear, timesAndOutputs) =>
        weekYear -> timesAndOutputs.map(_._2).sum
      }
      .toList.sortBy(_._1)

    // Print to console
    println("YearWeek\t\tTotal Energy Output")
    weeklyTotal.foreach { case (weekYear, total) =>
      println(f"$weekYear%-20s$total%.2f")
    }
    val dataList = weeklyTotal.map { case (weekYear, total) =>
      s"$weekYear\t\t$total"
    }
    // Calling the calculateStatistics function
    calculateStatistics(dataList)
    println("Weekly energy output data has been printed above.\n")
  }
}