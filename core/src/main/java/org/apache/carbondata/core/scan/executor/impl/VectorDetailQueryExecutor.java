/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.scan.executor.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.VectorDetailQueryResultIterator;

import org.apache.hadoop.conf.Configuration;

/**
 * Below class will be used to execute the detail query and returns columnar vectors.
 */
public class VectorDetailQueryExecutor extends AbstractQueryExecutor<Object> {

  public VectorDetailQueryExecutor(Configuration configuration) {
    super(configuration);
  }

  @Override
  public CarbonIterator<Object> execute(QueryModel queryModel)
      throws QueryExecutionException, IOException {
    this.setExecutorService(Executors.newCachedThreadPool());
    List<BlockExecutionInfo> blockExecutionInfoList = getBlockExecutionInfos(queryModel);
    this.queryIterator = new VectorDetailQueryResultIterator(
        blockExecutionInfoList,
        queryModel,
        queryProperties.executorService
    );
    return this.queryIterator;
  }

}
