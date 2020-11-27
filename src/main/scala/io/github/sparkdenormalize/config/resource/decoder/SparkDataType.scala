package io.github.sparkdenormalize.config.resource.decoder

import scala.collection.JavaConverters._

import io.circe.{Decoder, DecodingFailure, HCursor, Json}
import org.apache.spark.sql.types._

object SparkDataType {

  implicit val decodeDataType: Decoder[DataType] =
    new Decoder[DataType] {
      final def apply(c: HCursor): Decoder.Result[DataType] = {

        // get the keys of this JSON object
        val jsonKeys: Seq[String] = c.keys match {
          case Some(value: Iterable[String]) => value.toSeq
          case None =>
            return Left( // scalastyle:ignore return
              DecodingFailure(
                "JSON representing DataType must be an object, not an array",
                c.history
              )
            )
        }

        // the YAML is only sensible if there is a single key (representing a single DataType)
        if (jsonKeys.size == 1) {
          jsonKeys.head match {
            case "array"            => c.get[ArrayType]("array")
            case "binary"           => Right(DataTypes.BinaryType)
            case "boolean"          => Right(DataTypes.BooleanType)
            case "byte"             => Right(DataTypes.ByteType)
            case "calendarInterval" => Right(DataTypes.CalendarIntervalType)
            case "date"             => Right(DataTypes.DateType)
            case "decimal"          => c.get[DecimalType]("decimal")
            case "double"           => Right(DataTypes.DoubleType)
            case "float"            => Right(DataTypes.FloatType)
            case "integer"          => Right(DataTypes.IntegerType)
            case "long"             => Right(DataTypes.LongType)
            case "map"              => c.get[MapType]("map")
            case "null"             => Right(DataTypes.NullType)
            case "short"            => Right(DataTypes.ShortType)
            case "string"           => Right(DataTypes.StringType)
            case "struct"           => c.get[StructType]("struct")
            case "timestamp"        => Right(DataTypes.TimestampType)
          }
        } else {
          Left(
            DecodingFailure(
              s"JSON object representing DataType must have exactly 1 key, but got: [${jsonKeys.mkString(", ")}]",
              c.history
            )
          )
        }

      }
    }

  implicit val decodeArrayType: Decoder[ArrayType] =
    new Decoder[ArrayType] {
      final def apply(c: HCursor): Decoder.Result[ArrayType] =
        for {
          elementType <- c.get[DataType]("elementType")
          containsNull <- c.getOrElse[Boolean]("containsNull")(true)
        } yield {
          DataTypes.createArrayType(elementType, containsNull)
        }
    }

  implicit val decodeDecimalType: Decoder[DecimalType] =
    new Decoder[DecimalType] {
      final def apply(c: HCursor): Decoder.Result[DecimalType] =
        for {
          precision <- c.get[Int]("precision")
          scale <- c.get[Int]("scale")
        } yield {
          DataTypes.createDecimalType(precision, scale)
        }
    }

  implicit val decodeMapType: Decoder[MapType] =
    new Decoder[MapType] {
      final def apply(c: HCursor): Decoder.Result[MapType] =
        for {
          keyType <- c.get[DataType]("keyType")
          valueType <- c.get[DataType]("valueType")
          valueContainsNull <- c.getOrElse[Boolean]("valueContainsNull")(true)
        } yield {
          DataTypes.createMapType(keyType, valueType, valueContainsNull)
        }
    }

  implicit val decodeStructType: Decoder[StructType] =
    new Decoder[StructType] {
      final def apply(c: HCursor): Decoder.Result[StructType] =
        for {
          fields <- c.get[List[StructField]]("fields")
        } yield {
          DataTypes.createStructType(fields.asJava)
        }
    }

  implicit val decodeStructField: Decoder[StructField] =
    new Decoder[StructField] {
      final def apply(c: HCursor): Decoder.Result[StructField] =
        for {
          name <- c.get[String]("name")
          dataType <- c.get[DataType]("dataType")
          nullable <- c.getOrElse[Boolean]("nullable")(true)
          metadata <- c.getOrElse[Metadata]("metadata")(Metadata.empty)
        } yield {
          DataTypes.createStructField(name, dataType, nullable, metadata)
        }
    }

  implicit val decodeMetadata: Decoder[Metadata] =
    Decoder.decodeJson.emap { json: Json =>
      try {
        Right(Metadata.fromJson(json.noSpaces))
      } catch {
        case ex: Exception =>
          Left(ex.getMessage)
      }
    }

}
