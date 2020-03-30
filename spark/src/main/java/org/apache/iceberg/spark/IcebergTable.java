/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.spark.source.SparkIcebergTable;
import org.apache.spark.sql.catalyst.expressions.Expression;

public interface IcebergTable {

  /**
   * A placeholder for Iceberg table.
   *
   * @param tableName String name of table, can be "a/b/c" as Hadoop table, or "a.b" as Hive table.
   */
  static IcebergTable of(String tableName) {
    return new SparkIcebergTable(tableName);
  }

  /**
   * Delete data from the table that match the given `condition`.
   *
   * @param conditionExpr Boolean SQL expression
   */
  void delete(Expression conditionExpr);

  /**
   *
   * Delete data from the table that match the given `condition`.
   *
   * @param condition Boolean String expression
   */
  void delete(String condition);

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   *
   * {{{
   *    sparkTable.update(
   *      "date > '2018-01-01'",
   *      new HashMap&lt;String, String>() {{
   *        put("data", "data + 1");
   *      }}
   *    );
   * }}}
   *
   * @param condition boolean expression as SQL formatted string object specifying
   *                  which rows to update.
   * @param assignments rules to update a row as a Java map between target column names and
   *                    corresponding update expressions as SQL formatted strings.
   */
  void update(String condition, Map<String, String> assignments);

  /**
   * Update data from the table on the rows that match the given `condition`,
   * which performs the rules defined by `set`.
   *
   * {{{
   *    sparkTable.update(
   *      "date > '2018-01-01'",
   *      new HashMap&lt;String, String>() {{
   *        put("data", functions.expr("gt3").expr());
   *      }}
   *    );
   * }}}
   *
   * @param condition boolean expression as SQL formatted string object specifying
   *                  which rows to update.
   * @param assignments rules to update a row as a Java map between target column names and
   *                    corresponding update expressions as SQL formatted strings.
   */
  void updateExpr(String condition, Map<String, Expression> assignments);
}
