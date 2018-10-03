package com.sksamuel.avro4s.record.decoder

import com.sksamuel.avro4s.{AvroSchema, Decoder}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.scalatest.{FunSuite, Matchers}

class TransientDecoderTest extends FunSuite with Matchers {

  case class TransientFoo(a: String, @transient b: Option[String], @transient c: String)

  test("decoder should populate transient fields with None or null") {
    val schema = AvroSchema[TransientFoo]
    val record = new GenericData.Record(schema)
    record.put("a", new Utf8("hello"))
    Decoder[TransientFoo].decode(record) shouldBe TransientFoo("hello", None, null)
  }
}
