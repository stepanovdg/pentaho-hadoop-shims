/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.hadoop.shim.hdp21.delegating;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.hdp21.authorization.HadoopAuthorizationService;
import org.pentaho.hadoop.shim.hdp21.authorization.HasHadoopAuthorizationService;
import org.pentaho.yarn.shim.api.KettleYarnClient;
import org.pentaho.yarn.shim.api.KettleYarnClientException;
import org.pentaho.yarn.shim.api.KettleYarnClientFactory;

public class DelegatingKettleYarnClientFactory implements KettleYarnClientFactory, HasHadoopAuthorizationService {
  private KettleYarnClientFactory delegate;

  @Override
  public ShimVersion getVersion() {
    return delegate.getVersion();
  }

  @Override
  public void setHadoopAuthorizationService( HadoopAuthorizationService hadoopAuthorizationService ) throws Exception {
    delegate = hadoopAuthorizationService.getShim( KettleYarnClientFactory.class );
  }

  @Override
  public KettleYarnClient createKettleYarnClient( LogChannelInterface log, String defaultFs, String stagingDir,
                                                  int nodes, int port, int nodeMemory, int coresPerNode,
                                                  String carteUser, String cartePassword, String clusterSchemaName,
                                                  String fileName ) throws KettleYarnClientException {
    return delegate
      .createKettleYarnClient( log, defaultFs, stagingDir, nodes, port, nodeMemory, coresPerNode, carteUser,
        cartePassword, clusterSchemaName, fileName );
  }
}
