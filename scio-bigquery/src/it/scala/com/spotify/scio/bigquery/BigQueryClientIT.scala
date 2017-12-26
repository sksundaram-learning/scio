/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.bigquery

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class BigQueryClientIT extends AsyncFlatSpec with Matchers {

  implicit override def executionContext: ExecutionContext = ExecutionContext.Implicits.global

  private val bq = BigQueryClient.defaultInstance()

  val legacyQuery =
    "SELECT word, word_count FROM [bigquery-public-data:samples.shakespeare] LIMIT 10"
  val sqlQuery =
    "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10"

  "extractLocation" should "work with legacy syntax" in {
    val query = "SELECT word FROM [data-integration-test:samples_%s.shakespeare]"
    val f = Future.sequence(Seq(
      Future(bq.extractLocation(query.format("us"))),
      Future(bq.extractLocation(query.format("eu")))))
    f.map(_ shouldBe Seq(Some("US"), Some("EU")))
  }

  it should "work with SQL syntax" in {
    val query = "SELECT word FROM `data-integration-test.samples_%s.shakespeare`"
    val f = Future.sequence(Seq(
      Future(bq.extractLocation(query.format("us"))),
      Future(bq.extractLocation(query.format("eu")))))
    f.map(_ shouldBe Seq(Some("US"), Some("EU")))
  }

  it should "support missing source tables" in {
    Future(bq.extractLocation("SELECT 6")).map(_ shouldBe None)
  }


  "extractTables" should "work with legacy syntax" in {
    val tableSpec = BigQueryHelpers.parseTableSpec("bigquery-public-data:samples.shakespeare")
    Future(bq.extractTables(legacyQuery)).map(_ shouldBe Set(tableSpec))
  }

  it should "work with SQL syntax" in {
    val tableSpec = BigQueryHelpers.parseTableSpec("bigquery-public-data:samples.shakespeare")
    Future(bq.extractTables(sqlQuery)).map(_ shouldBe Set(tableSpec))
  }

  "getQuerySchema" should "work with legacy syntax" in {
    val expected = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING").setMode("REQUIRED"),
      new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("REQUIRED")
    ).asJava)
    Future(bq.getQuerySchema(legacyQuery)).map(_ shouldBe expected)
  }

  it should "work with SQL syntax" in {
    val expected = new TableSchema().setFields(List(
      new TableFieldSchema().setName("word").setType("STRING").setMode("NULLABLE"),
      new TableFieldSchema().setName("word_count").setType("INTEGER").setMode("NULLABLE")
    ).asJava)
    Future(bq.getQuerySchema(sqlQuery)).map(_ shouldBe expected)
  }

  it should "fail invalid legacy syntax" in {
    val f = Future(bq.getQuerySchema(
      "SELECT word, count FROM [bigquery-public-data:samples.shakespeare]"))
    f.failed.map { case e: GoogleJsonResponseException =>
      e.getDetails.getCode shouldBe 400
    }
  }

  it should "fail invalid SQL syntax" in {
    val f = Future(bq.getQuerySchema(
      "SELECT word, count FROM `bigquery-public-data.samples.shakespeare`"))
    f.failed.map { case e: GoogleJsonResponseException =>
      e.getDetails.getCode shouldBe 400
    }
  }

  "getQueryRows" should "work with legacy syntax" in {
    Future(bq.getQueryRows(legacyQuery).toList).map { rows =>
      rows.size shouldBe 10
      all(rows.map(_.keySet().asScala)) shouldBe Set("word", "word_count")
    }
  }

  it should "work with SQL syntax" in {
    Future(bq.getQueryRows(sqlQuery).toList).map { rows =>
      rows.size shouldBe 10
      all(rows.map(_.keySet().asScala)) shouldBe Set("word", "word_count")
    }
  }

  "getTableSchema" should "work" in {
    Future(bq.getTableSchema("bigquery-public-data:samples.shakespeare")).map { schema =>
      val fields = schema.getFields.asScala
      fields.size shouldBe 4
      fields.map(_.getName) shouldBe Seq("word", "word_count", "corpus", "corpus_date")
      fields.map(_.getType) shouldBe Seq("STRING", "INTEGER", "STRING", "INTEGER")
      fields.map(_.getMode) shouldBe Seq("REQUIRED", "REQUIRED", "REQUIRED", "REQUIRED")
    }
  }

  "getTableRows" should "work" in {
    val columns = Set("word", "word_count", "corpus", "corpus_date")
    Future(bq.getTableRows("bigquery-public-data:samples.shakespeare").take(10).toList)
      .map { rows => all(rows.map(_.keySet().asScala)) shouldBe columns }
  }

}
