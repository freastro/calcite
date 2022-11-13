/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Unit test for sequence support.
 */
public class SequenceTest {

  private Connection db;

  @AfterEach void afterEach() {
    if (db != null) {
      try {
        db.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  @Test public void testBigint() {
    final String currentValue = "SELECT CURRENT VALUE FOR S1";
    final String nextValue = "SELECT NEXT VALUE FOR S1";
    final long start = 8000000000L;

    final CalciteAssert.AssertThat that = that(
        "CREATE SEQUENCE S1 AS BIGINT START WITH " + start);
    that.query(currentValue)
        .explainContains("PLAN=JdbcToEnumerableConverter\n" +
            "  JdbcProject(EXPR$0=[CURRENT_VALUE(JdbcTableScan(table=[[adhoc, S1]])\n)])\n");
    that.query(nextValue)
        .explainContains("PLAN=JdbcToEnumerableConverter\n" +
            "  JdbcProject(EXPR$0=[NEXT_VALUE(JdbcTableScan(table=[[adhoc, S1]])\n)])\n");

    that.query(nextValue)
        .returns(scalarValue(is(
            both(instanceOf(Long.class))
                .and(equalTo(start)))));
    that.query(currentValue)
        .returns(scalarValue(is(
            both(instanceOf(Long.class))
                .and(equalTo(start)))));
    that.query(nextValue)
        .returns(scalarValue(is(start + 1)));
    that.query(currentValue)
        .returns(scalarValue(is(start + 1)));
  }

  @Test public void testInt() {
    final String currentValue = "SELECT CURRENT VALUE FOR S2";
    final String nextValue = "SELECT NEXT VALUE FOR S2";

    final CalciteAssert.AssertThat that = that(
        "CREATE SEQUENCE S2 AS INT START WITH 0");
    that.query(currentValue)
        .explainContains("PLAN=JdbcToEnumerableConverter\n" +
            "  JdbcProject(EXPR$0=[CURRENT_VALUE(JdbcTableScan(table=[[adhoc, S2]])\n)])\n");
    that.query(nextValue)
        .explainContains("PLAN=JdbcToEnumerableConverter\n" +
            "  JdbcProject(EXPR$0=[NEXT_VALUE(JdbcTableScan(table=[[adhoc, S2]])\n)])\n");

    that.query(nextValue)
        .returns(scalarValue(is(
            both(instanceOf(Integer.class))
                .and(equalTo(0)))));
    that.query(currentValue)
        .returns(scalarValue(is(
            both(instanceOf(Integer.class))
                .and(equalTo(0)))));
    that.query(nextValue)
        .returns(scalarValue(is(1)));
    that.query(currentValue)
        .returns(scalarValue(is(1)));
  }

  private <T> Consumer<ResultSet> scalarValue(Matcher<? super T> matcher) {
    return rs -> {
      try {
        assertThat(rs.next(), is(true));
        final Object value = rs.getObject(1);
        assertThat(value, (Matcher) matcher);
      } catch (SQLException e) {
        throw new RuntimeException("Failed to check scalar value", e);
      }
    };
  }

  private CalciteAssert.AssertThat that(String... sql) {
    // Setup connection
    final String url = "jdbc:hsqldb:mem:test" + System.identityHashCode(this);
    try {
      db = DriverManager.getConnection(url + ";shutdown=true");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to setup connection", e);
    }

    // Initialize database
    try (final Statement statement = db.createStatement()) {
      for (final String query : sql) {
        try {
          statement.execute(query);
        } catch (SQLException e) {
          throw new RuntimeException("Failed to execute query: " + query, e);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize database", e);
    }

    // Return Calcite connection
    return CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [\n"
        + "     {\n"
        + "       type: 'jdbc',\n"
        + "       name: 'adhoc',\n"
        + "       jdbcDriver: 'org.hsqldb.jdbcDriver',\n"
        + "       jdbcUrl: '" + url + "',\n"
        + "       jdbcCatalog: null,\n"
        + "       jdbcSchema: null\n"
        + "     }\n"
        + "  ]\n"
        + "}");
  }
}
