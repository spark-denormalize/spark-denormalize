package io.github.sparkdenormalize.config.resource.spec

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.github.sparkdenormalize.config.resource.decoder.SparkColumn.decodeColumn
import io.github.sparkdenormalize.config.resource.decoder.SparkDataType.decodeDataType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{DataType, StringType}

/** A relationship between DataSources, based field values */
case class DataRelationshipSpec(
    representations: List[DataRelationshipSpec.Representation]
) extends ResourceSpec {
  require(representations.nonEmpty, "`representations` must be non-empty")

  private val firstJoinKeySize: Long = representations.head.joinKey.size
  private val firstJoinKeyCastDataTypes: List[DataType] =
    representations.head.joinKey.map(_.castDataType)
  require(
    representations.forall(_.joinKey.size == firstJoinKeySize),
    s"`joinKey[]` must have the same number of elements for all `spec.representations[]"
  )
  require(
    representations.forall(_.joinKey.map(_.castDataType) == firstJoinKeyCastDataTypes),
    s"`joinKey[]` must have the same `castDataType` (at each index) for all `spec.representations[]`"
  )

}

object DataRelationshipSpec {

  // tells IDE's we need these Decoder in scope so they wont remove it as an 'unused' import
  implicitly[Decoder[Column]]
  implicitly[Decoder[DataType]]

  // ========================
  // Circe Decoders
  // ========================
  implicit val decodeDataRelationshipSpec: Decoder[DataRelationshipSpec] =
    new Decoder[DataRelationshipSpec] {
      final def apply(c: HCursor): Decoder.Result[DataRelationshipSpec] =
        for {
          representations <- c.get[List[Representation]]("representations")
        } yield {
          try {
            DataRelationshipSpec(representations = representations)
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeRepresentation: Decoder[Representation] =
    new Decoder[Representation] {
      final def apply(c: HCursor): Decoder.Result[Representation] =
        for {
          dataSources <- c.get[List[DataSourceReference]]("dataSources")
          joinKey <- c.get[List[RelationshipJoinKeyComponent]]("joinKey")
        } yield {
          try {
            Representation(
              dataSources = dataSources,
              joinKey = joinKey
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeRelationshipJoinKeyComponent: Decoder[RelationshipJoinKeyComponent] =
    new Decoder[RelationshipJoinKeyComponent] {
      final def apply(c: HCursor): Decoder.Result[RelationshipJoinKeyComponent] =
        for {
          fields <- c.get[List[RelationshipJoinKeyComponentField]]("fields")
          query <- c.get[Option[Column]]("query")
          castDataType <- c.getOrElse[DataType]("castDataType")(StringType)
        } yield {
          try {
            RelationshipJoinKeyComponent(
              fields = fields,
              query = query.getOrElse(
                // if no `query` was passed, set default value: "fields[0].fieldName"
                if (fields.nonEmpty) {
                  if (fields.size == 1) {
                    expr(fields.head.fieldName)
                  } else {
                    throw new IllegalArgumentException(
                      "`fields` must contain exactly 1 element if `query` is omitted"
                    )
                  }
                } else {
                  throw new IllegalArgumentException("`fields` must be non-empty")
                }
              ),
              castDataType = castDataType
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeRelationshipJoinKeyComponentField: Decoder[RelationshipJoinKeyComponentField] =
    new Decoder[RelationshipJoinKeyComponentField] {
      final def apply(c: HCursor): Decoder.Result[RelationshipJoinKeyComponentField] =
        for {
          fieldName <- c.get[String]("fieldName")
          dataType <- c.get[DataType]("dataType")
        } yield {
          try {
            RelationshipJoinKeyComponentField(
              fieldName = fieldName,
              dataType = dataType
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  // ========================
  // Classes
  // ========================
  case class Representation(
      dataSources: List[DataSourceReference],
      joinKey: List[RelationshipJoinKeyComponent]
  ) {
    require(dataSources.nonEmpty, "`dataSources` must be non-empty")
    require(joinKey.nonEmpty, "`joinKey` must be non-empty")
  }

  case class RelationshipJoinKeyComponent(
      fields: List[RelationshipJoinKeyComponentField],
      query: Column, // default: "fields[0].fieldName"
      castDataType: DataType // default: StringType
  ) {
    require(fields.nonEmpty, "`fields` must be non-empty")

    private val fieldsFieldNames: Set[String] = fields.map(_.fieldName).toSet
    private val queryFieldNames: Set[String] = query.expr.references.map(_.name).toSet
    require(
      fieldsFieldNames.map(_.toLowerCase) == queryFieldNames.map(_.toLowerCase),
      s"`query` must exactly reference all fields declared in `fields[]` (case-insensitive), but got: " +
        s"`fields[]` = [${fieldsFieldNames.mkString(", ")}] & " +
        s"`query` = [${queryFieldNames.mkString(", ")}]"
    )
  }

  case class RelationshipJoinKeyComponentField(
      fieldName: String,
      dataType: DataType
  ) {
    require(fieldName.nonEmpty, "`fieldName` must be non-empty")
  }

}
