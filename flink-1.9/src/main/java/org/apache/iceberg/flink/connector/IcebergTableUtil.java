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

package org.apache.iceberg.flink.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableUtil {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableUtil.class);

  public static final String EMPTY_STRING = "";

  private IcebergTableUtil() {}

  /**
   * Find and return {@link Table} based on the given {@link Configuration}.
   * The identifier in the configuration provided is used to decide the source from which the table is loaded:
   * (1) If the identifier contains "/", like a path, the table is loaded by {@link HadoopTables};
   * (2) For other cases, the table is loaded by {@link HiveCatalog}.
   *
   * @param config configurations containing table identifier and other necessary parameters to find the named table.
   * @return {@link Table} instance loaded by {@link HiveCatalog} or {@link HadoopTables}.
   */
  public static Table findTable(Configuration config) {
    // table identifier
    String identifier = config.getString(IcebergConnectorConstant.IDENTIFIER, EMPTY_STRING);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(identifier),
        "Table identifier not provided by parameter key of " + IcebergConnectorConstant.IDENTIFIER);

    // Use HiveCatalog or HadoopTables to load the table
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    if (identifier.contains("/")) {  // use HadoopTables
      HadoopTables hadoopTables = new HadoopTables(hadoopConf);
      Table table = hadoopTables.load(identifier /* as location */);

      LOG.info("Table loaded by HadoopTables with location as {}", identifier);
      return table;
    } else {  // use HiveCatalog
      // check Hive Metastore uris
      String hiveMetastoreUris = config.getString(IcebergConnectorConstant.HIVE_METASTORE_URIS, EMPTY_STRING);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(hiveMetastoreUris),
          "Hive Metastore uris not provided by parameter key of " + IcebergConnectorConstant.HIVE_METASTORE_URIS);
      hadoopConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);

      // load table
      HiveCatalog hiveCatalog = HiveCatalogs.loadCatalog(hadoopConf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(identifier);
      Table table = hiveCatalog.loadTable(tableIdentifier);

      LOG.info("Table of {} loaded from HiveCatalog", table);
      return table;
    }
  }
}
