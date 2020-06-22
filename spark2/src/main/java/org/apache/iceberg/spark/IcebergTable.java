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
import org.apache.iceberg.spark.source.IcebergMergeBuilder;
import org.apache.iceberg.spark.source.SparkIcebergTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
   * Apply an alias to the IcebergTable.
   *
   * @param alias the name as alias
   */
  IcebergTable as(String alias);

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

  /**
   * Builder to specify how to merge data from source DataFrame into the target table.
   * You can specify 1, 2 or 3 `when` clauses of which there can be at most 2 `whenMatched` clauses
   * and at most 1 `whenNotMatched` clause. Here are the constraints on these clauses.
   *
   *   - `whenMatched` clauses:
   *
   *     - There can be at most one `update` action and one `delete` action in `whenMatched` clauses.
   *
   *     - Each `whenMatched` clause can have an optional condition. However, if there are two
   *       `whenMatched` clauses, then the first one must have a condition.
   *
   *     - When there are two `whenMatched` clauses and there are conditions (or the lack of)
   *       such that a row matches both clauses, then the first clause/action is executed.
   *       In other words, the order of the `whenMatched` clauses matter.
   *
   *     - If none of the `whenMatched` clauses match a source-target row pair that satisfy
   *       the merge condition, then the target rows will not be updated or deleted.
   *
   *     - If you want to update all the columns of the target table with the
   *       corresponding column of the source DataFrame, then you can use the
   *       `whenMatched(...).updateAll()`. This is equivalent to
   *       <pre>
   *         whenMatched(...).updateExpr(Map(
   *           ("col1", "source.col1"),
   *           ("col2", "source.col2"),
   *           ...))
   *       </pre>
   *
   *   - `whenNotMatched` clauses:
   *
   *     - This clause can have only an `insert` action, which can have an optional condition.
   *
   *     - If the `whenNotMatched` clause is not present or if it is present but the non-matching
   *       source row does not satisfy the condition, then the source row is not inserted.
   *
   *     - If you want to insert all the columns of the target table with the
   *       corresponding column of the source DataFrame, then you can use
   *       `whenMatched(...).insertAll()`. This is equivalent to
   *       <pre>
   *         whenMatched(...).insertExpr(Map(
   *           ("col1", "source.col1"),
   *           ("col2", "source.col2"),
   *           ...))
   *       </pre>
   *
   * Scala example to update a key-value table with new key-values from a source DataFrame:
   * {{{
   *    table
   *     .as("target")
   *     .merge(source.as("source"))
   *     .onCondition("target.key = source.key")
   *     .whenMatched
   *     .updateExpr(Map(
   *       "value" -> "source.value"))
   *     .whenNotMatched
   *     .insertExpr(Map(
   *       "key" -> "source.key",
   *       "value" -> "source.value"))
   *     .execute()
   * }}}
   *
   * Java example to update a key-value table with new key-values from a source DataFrame:
   * {{{
   *    table
   *     .as("target")
   *     .merge(source.as("source"))
   *     .onCondition("target.key = source.key")
   *     .whenMatched
   *     .updateExpr(
   *        new HashMap&lt;String, String>() {{
   *          put("value", "source.value");
   *        }})
   *     .whenNotMatched
   *     .insertExpr(
   *        new HashMap&lt;String, String>() {{
   *         put("key", "source.key");
   *         put("value", "source.value");
   *       }})
   *     .execute();
   * }}}
   */
  IcebergMergeBuilder merge(Dataset<Row> source);
}
