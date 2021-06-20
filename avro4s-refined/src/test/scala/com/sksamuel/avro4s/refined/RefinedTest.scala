package com.sksamuel.avro4s.refined

import com.sksamuel.avro4s._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.types.numeric.NonNegInt
import eu.timepit.refined.types.string.NonEmptyString
import io.estatico.newtype.macros.newtype
import org.apache.avro.Schema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

case class Foo(nonEmptyStr: String Refined NonEmpty)
case class FooMap(nonEmptyStrKeyMap: Map[NonEmptyString, NonNegInt])

class RefinedTest extends AnyWordSpec with Matchers {

  "refinedSchemaFor" should {
    "use the schema for the underlying type" in {
      AvroSchema[Foo] shouldBe new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "Foo",
          |	"namespace": "com.sksamuel.avro4s.refined",
          |	"fields": [{
          |		"name": "nonEmptyStr",
          |		"type": "string"
          |	}]
          |}
        """.stripMargin)
    }

    "generate correct schemas for a Map when refined instances are in scope" in {
      case class Test(map: Map[String, Int], nonEmptyStr: String Refined NonEmpty)
      val schema = AvroSchema[Test]

      schema.getField("map").schema().getType shouldBe Schema.Type.MAP
      schema.getField("nonEmptyStr").schema().getType shouldBe Schema.Type.STRING
    }

    "Combined with newtypes" should {
      "just be the underlying type" in {
        object types {
          import scala.language.implicitConversions

          @newtype final case class Bar(nonEmptyStr: NonEmptyString)
          object Bar {
            implicit final val schema: SchemaFor[Bar] = SchemaFor[NonEmptyString].forType[Bar]
          }
        }

        AvroSchema[types.Bar] shouldBe AvroSchema[String]
      }
    }
  }

  "refinedStringMapKeySchemaFor" should {
    "use the schema for the underlying type" in {
      AvroSchema[FooMap] shouldBe new Schema.Parser().parse(
        """
          |{
          |	"type": "record",
          |	"name": "FooMap",
          |	"namespace": "com.sksamuel.avro4s.refined",
          |	"fields": [{
          |		"name": "nonEmptyStrKeyMap",
          |		"type": {
          |     "type": "map",
          |     "values": "int"
          |   }
          |	}]
          |}
        """.stripMargin)
    }
  }

  "refinedEncoder" should {
    "use the encoder for the underlying type" in {
      val expected: String Refined NonEmpty = "foo"
      val record = ToRecord[Foo].to(Foo(expected))
      record.get("nonEmptyStr").toString shouldBe expected.value
    }
  }

  "refinedStringMapKeyEncoder" should {
    "use the encoder for the underlying type" in {
      val key: NonEmptyString = "foo"
      val value: NonNegInt = 1
      val expected: Map[NonEmptyString, NonNegInt] = Map(key -> value)
      val record = ToRecord[FooMap].to(FooMap(expected))
      val encodedMap = record.get("nonEmptyStrKeyMap").asInstanceOf[java.util.Map[String, Int]]
      encodedMap.get(key.value) shouldBe value.value
    }
  }

  "refinedDecoder" should {
    "use the decoder for the underlying type" in {
      val expected: String Refined NonEmpty = "foo"
      val record = ImmutableRecord(AvroSchema[Foo], Vector(expected.value))
      FromRecord[Foo].from(record) shouldBe Foo(expected)
    }

    "throw when the value does not conform to the refined predicate" in {
      val record = ImmutableRecord(AvroSchema[Foo], Vector(""))
      assertThrows[IllegalArgumentException](FromRecord[Foo].from(record))
    }
  }

  "refinedStringMapKeyDecoder" should {
    "use the decoder for the underlying type" in {
      val key: NonEmptyString = "foo"
      val value: NonNegInt = 1

      val jMap = new java.util.HashMap[String, Int]()
      jMap.put(key.value, value.value)

      val expected = Map(key -> value)
      val record = ImmutableRecord(AvroSchema[FooMap], Vector(jMap))

      FromRecord[FooMap].from(record) shouldBe FooMap(expected)
    }
  }
}
