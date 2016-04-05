/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.csv.util

import java.sql.Timestamp
import java.util.{Date, Locale}
import java.text.SimpleDateFormat

import scala.util.control.Exception._
import scala.util.matching.Regex
import scala.collection.mutable.HashSet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

trait DateMatch {
  var dates = HashSet[String]()
  def createDateHash = {
    val minFourYear = 1700
    val maxFourYear = 3000
    val minTwoYear = 0
    val maxTwoYear = 99 
      for (year <- minFourYear to maxFourYear) {
          for (month <- 1 to 12) {
              for (day <- 1 to 31) {
                  val date = new StringBuilder()
                  date.append("%04d".format(year))
                  date.append("%02d".format(month))
                  date.append("%02d".format(day))
                  dates.add(date.toString())
              }
          }
      }
      for (year <- minFourYear to maxFourYear) {
          for (month <- 1 to 12) {
              for (day <- 1 to 31) {
                  val date = new StringBuilder()
                  date.append("%02d-".format(day))
                  date.append("%02d-".format(month))
                  date.append("%04d".format(year))
                  dates.add(date.toString())
              }
          }
      }
      for (year <- minFourYear to maxFourYear) {
          for (month <- 1 to 12) {
              for (day <- 1 to 31) {
                  val date = new StringBuilder()
                  date.append("%02d/".format(day))
                  date.append("%02d/".format(month))
                  date.append("%04d".format(year))
                  dates.add(date.toString())
              }
          }
      }
      for (year <- minTwoYear to minTwoYear) {
          for (month <- 1 to 12) {
              for (day <- 1 to 31) {
            val date = new StringBuilder()
            date.append("%02d".format(year))
            date.append("%02d".format(month))
            date.append("%02d".format(day))
            dates.add(date.toString())
          }
        }
      }
      for (year <- minTwoYear to minTwoYear) {
          for (month <- 1 to 12) {
              for (day <- 1 to 31) {
            val date = new StringBuilder()
            date.append("%02d/".format(day))
            date.append("%02d/".format(month))
            date.append("%02d".format(year))
            dates.add(date.toString())
          }
        }
      }
  }
  createDateHash

  val dateRegex = Array("([a-zA-z]{3,9})([ ])([0-9]{1,2})([, ])([0-9]{4})".r,
                   "([0-9]{1,2})([/])([0-9]{1,2})([/])([0-9]{2})".r,
                   //"([0-9]{1,2})([/])([0-9]{1,2})([/])([0-9]{4})".r,
                   "([0-9]{1,2})([-])([0-9]{1,2})([-])([0-9]{2})".r,
                   //"([0-9]{1,2})([-])([0-9]{1,2})([-])([0-9]{4})".r,
                   "([0-9]{4})([-])([0-9]{1,2})([-])([0-9]{1,2})".r,
                   "([0-9]{4})([-])([a-zA-z]{3})([-])([0-9]{2})".r,
                   "([a-zA-z]{3})([-])([0-9]{1,2})([-])([0-9]{4})".r,
                   "([0-9]{2})([-])([a-zA-z]{3})([-])([0-9]{4})".r)
}

case class DataStruct(dataType: DataType, dataSize: Integer)

private[csv] object InferSchema extends DateMatch {

  /**
   * Similar to the JSON schema inference.
   * [[org.apache.spark.sql.execution.datasources.json.InferSchema]]
   *     1. Infer type of each row
   *     2. Merge row types to find common type
   *     3. Replace any null types with string type
   */
  def apply(
      tokenRdd: RDD[Array[String]],
      header: Array[String],
      nullValue: String = "",
      dateFormatter: SimpleDateFormat = null): StructType = {

    val startType: Array[DataStruct] = Array.fill[DataStruct](header.length)(DataStruct(NullType, 0))
    val rootTypes: Array[DataStruct] = tokenRdd.aggregate(startType)(
      inferRowType(nullValue, dateFormatter),
      mergeRowTypes)

    val structFields = header.zip(rootTypes).map { case (thisHeader, rootType) =>
      val dType = rootType.dataType match {
        case z: NullType => StringType
        case other => other
      }
      val metadata = new MetadataBuilder().putLong("maxlength", rootType.dataSize.toLong).build()
      StructField(thisHeader, dType, nullable = true, metadata)
    }

    StructType(structFields)
  }

  private def inferRowType(nullValue: String, dateFormatter: SimpleDateFormat)
  (rowSoFar: Array[DataStruct], next: Array[String]): Array[DataStruct] = {
    var i = 0
    while (i < math.min(rowSoFar.length, next.length)) {  // May have columns on right missing.
      rowSoFar(i) = DataStruct(inferField(rowSoFar(i).dataType, next(i), nullValue), 
                               if (rowSoFar(i).dataSize >= next(i).length) rowSoFar(i).dataSize 
                               else next(i).length)
      i+=1
    }
    rowSoFar
  }
  private[csv] def mergeRowTypes(
      first: Array[DataStruct],
      second: Array[DataStruct]): Array[DataStruct] = {
    first.zipAll(second, DataStruct(NullType, 0), DataStruct(NullType, 0)).map { case ((a, b)) =>
      val bestType = findTightestCommonType(a.dataType, b.dataType).getOrElse(NullType)
      val largestSize = if (a.dataSize >= b.dataSize) a.dataSize else b.dataSize
      DataStruct(bestType, largestSize)
    }
  }

  /**
   * Infer type of string field. Given known type Double, and a string "1", there is no
   * point checking if it is an Int, as the final type must be Double or higher.
   */
  private[csv] def inferField(typeSoFar: DataType,
      field: String,
      nullValue: String = "",
      dateFormatter: SimpleDateFormat = null): DataType = {
    def tryParseInteger(field: String): DataType = if ((allCatch opt field.toInt).isDefined) {
      IntegerType
    } else {
      tryParseLong(field)
    }

    def tryParseLong(field: String): DataType = if ((allCatch opt field.toLong).isDefined) {
      LongType
    } else {
      tryParseDouble(field)
    }

    def tryParseDouble(field: String): DataType = {
      if ((allCatch opt field.toDouble).isDefined) {
        DoubleType
      } else {
        tryParseTimestamp(field)
      }
    }

    def tryParseTimestamp(field: String): DataType = {
      if (dateFormatter != null) {
        // This case infers a custom `dataFormat` is set.
        if ((allCatch opt dateFormatter.parse(field)).isDefined){
          TimestampType
        } else {
          tryParseBoolean(field)
        }
      } else {
        // We keep this for backwords competibility.
        if ((allCatch opt Timestamp.valueOf(field)).isDefined) {
          TimestampType
        } else {
          tryParseBoolean(field)
        }
      }
    }

    def tryParseDate(field: String): DataType = {
      var isDate : Boolean = false

      dateRegex.foreach(_.findFirstIn(field).map(d => if (d.length == field.length) isDate = true))

      if (isDate == false)
        if (dates.contains(field)) isDate = true

      if (isDate == true) {
        DateType
      } else {
        tryParseInteger(field)
      }
    }

    def tryParseBoolean(field: String): DataType = {
      if ((allCatch opt field.toBoolean).isDefined) {
        BooleanType
      } else {
        stringType()
      }
    }

    // Defining a function to return the StringType constant is necessary in order to work around
    // a Scala compiler issue which leads to runtime incompatibilities with certain Spark versions;
    // see issue #128 for more details.
    def stringType(): DataType = {
      StringType
    }

    if (field == null || field.isEmpty || field == nullValue) {
      typeSoFar
    } else {
      typeSoFar match {
        case NullType => tryParseDate(field)
        case DateType => tryParseDate(field)
        case IntegerType => tryParseInteger(field)
        case LongType => tryParseLong(field)
        case DoubleType => tryParseDouble(field)
        case TimestampType => tryParseTimestamp(field)
        case BooleanType => tryParseBoolean(field)
        case StringType => StringType
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
    }
  }

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
   */
  private val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      DateType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType,
      DecimalType.Unlimited)

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion]]
   */
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)
    case (StringType, t2) => Some(StringType)
    case (t1, StringType) => Some(StringType)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }
}
