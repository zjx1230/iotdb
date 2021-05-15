/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.router;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_SLIDE_STEP;
import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_WINDOW_RANGE;
import static org.apache.iotdb.db.index.common.IndexConstant.PAA_DIM;
import static org.apache.iotdb.db.index.common.IndexType.ELB_INDEX;
import static org.apache.iotdb.db.index.common.IndexType.NO_INDEX;
import static org.apache.iotdb.db.index.common.IndexType.RTREE_PAA;

public class ProtoIndexRouterTest {

  private static final String storageGroupSub = "root.wind1";
  private static final String storageGroupFull = "root.wind2";
  private static final String speed1 = "root.wind1.azq01.speed";
  private static final String direction1 = "root.wind2.1.direction";
  private static final String direction2 = "root.wind2.2.direction";
  private static final String direction3 = "root.wind2.3.direction";
  private static final String index_sub = speed1;
  private static final String index_full = "root.wind2.*.direction";
  private IndexInfo infoFull;
  private IndexInfo infoSub;
  private CreateIndexProcessorFunc fakeCreateFunc;
  private IndexInfo infoFull2;

  private void prepareMManager() throws MetadataException {
    MManager mManager = MManager.getInstance();
    mManager.init();
    mManager.setStorageGroup(new PartialPath(storageGroupSub));
    mManager.setStorageGroup(new PartialPath(storageGroupFull));
    mManager.createTimeseries(
        new PartialPath(speed1),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    mManager.createTimeseries(
        new PartialPath(direction1),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    mManager.createTimeseries(
        new PartialPath(direction2),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    mManager.createTimeseries(
        new PartialPath(direction3),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    Map<String, String> props_sub = new HashMap<>();
    props_sub.put(INDEX_WINDOW_RANGE, "4");
    props_sub.put(INDEX_SLIDE_STEP, "8");

    Map<String, String> props_full = new HashMap<>();
    props_full.put(PAA_DIM, "5");
    Map<String, String> props_full2 = new HashMap<>();
    props_full2.put(PAA_DIM, "10");
    this.infoSub = new IndexInfo(ELB_INDEX, 0, props_sub);
    this.infoFull = new IndexInfo(RTREE_PAA, 5, props_full);
    this.infoFull2 = new IndexInfo(NO_INDEX, 10, props_full2);
    this.fakeCreateFunc =
        (indexSeries, indexInfoMap) ->
            new IndexProcessor(
                indexSeries,
                testRouterDir + File.separator + "index_fake_" + indexSeries,
                indexInfoMap);
  }

  private static final String testRouterDir = "test_protoIndexRouter";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    if (!new File(testRouterDir).exists()) {
      new File(testRouterDir).mkdirs();
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    if (new File(testRouterDir).exists()) {
      FileUtils.deleteDirectory(new File(testRouterDir));
    }
  }

  @Test
  public void testBasic() throws MetadataException, IOException {
    prepareMManager();
    ProtoIndexRouter router = new ProtoIndexRouter(testRouterDir);

    router.addIndexIntoRouter(new PartialPath(index_sub), infoSub, fakeCreateFunc, true);
    router.addIndexIntoRouter(new PartialPath(index_full), infoFull, fakeCreateFunc, true);
    router.addIndexIntoRouter(new PartialPath(index_full), infoFull2, fakeCreateFunc, true);
    //    System.out.println(router.toString());
    Assert.assertEquals(
        "<{NO_INDEX=[type: NO_INDEX, time: 10, props: {PAA_DIM=10}], RTREE_PAA=[type: RTREE_PAA, time: 5, props: {PAA_DIM=5}]}\n"
            + "root.wind2.*.direction: {NO_INDEX=NO_INDEX, RTREE_PAA=nMax:50,nMin:2,dim:4,seedsPicker:LINEARinner:0,leaf:1,item:0;\n"
            + " calc size: 52=4*3+2+(#inner(0)+#leaf(1)) * (2*dim(4)*4+2+4) + #item(0) * (dim(4)*4 + 2 + 8)}>\n"
            + "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {INDEX_SLIDE_STEP=8, INDEX_WINDOW_RANGE=4}]}\n"
            + "root.wind1.azq01.speed: {ELB_INDEX=[]}>\n",
        router.toString());
    // select by storage group
    ProtoIndexRouter sgRouters =
        (ProtoIndexRouter) router.getRouterByStorageGroup(storageGroupFull);
    Assert.assertEquals(
        "<{NO_INDEX=[type: NO_INDEX, time: 10, props: {PAA_DIM=10}], RTREE_PAA=[type: RTREE_PAA, time: 5, props: {PAA_DIM=5}]}\n"
            + "root.wind2.*.direction: {NO_INDEX=NO_INDEX, RTREE_PAA=nMax:50,nMin:2,dim:4,seedsPicker:LINEARinner:0,leaf:1,item:0;\n"
            + " calc size: 52=4*3+2+(#inner(0)+#leaf(1)) * (2*dim(4)*4+2+4) + #item(0) * (dim(4)*4 + 2 + 8)}>\n",
        sgRouters.toString());
    //    System.out.println(sgRouters.toString());

    // serialize
    router.serialize(false);
    //    System.out.println(router.toString());
    Assert.assertEquals(
        "<{NO_INDEX=[type: NO_INDEX, time: 10, props: {PAA_DIM=10}], RTREE_PAA=[type: RTREE_PAA, time: 5, props: {PAA_DIM=5}]}\n"
            + "root.wind2.*.direction: {NO_INDEX=NO_INDEX, RTREE_PAA=nMax:50,nMin:2,dim:4,seedsPicker:LINEARinner:0,leaf:1,item:0;\n"
            + " calc size: 52=4*3+2+(#inner(0)+#leaf(1)) * (2*dim(4)*4+2+4) + #item(0) * (dim(4)*4 + 2 + 8)}>\n"
            + "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {INDEX_SLIDE_STEP=8, INDEX_WINDOW_RANGE=4}]}\n"
            + "root.wind1.azq01.speed: {ELB_INDEX=[]}>\n",
        router.toString());
    router.serialize(true);
    Assert.assertEquals("", router.toString());

    IIndexRouter newRouter = new ProtoIndexRouter(testRouterDir);
    newRouter.deserializeAndReload(fakeCreateFunc);
    Assert.assertEquals(
        "<{NO_INDEX=[type: NO_INDEX, time: 10, props: {PAA_DIM=10}], RTREE_PAA=[type: RTREE_PAA, time: 5, props: {PAA_DIM=5}]}\n"
            + "root.wind2.*.direction: {NO_INDEX=NO_INDEX, RTREE_PAA=nMax:50,nMin:2,dim:4,seedsPicker:LINEARinner:0,leaf:1,item:0;\n"
            + " calc size: 52=4*3+2+(#inner(0)+#leaf(1)) * (2*dim(4)*4+2+4) + #item(0) * (dim(4)*4 + 2 + 8)}>\n"
            + "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {INDEX_SLIDE_STEP=8, INDEX_WINDOW_RANGE=4}]}\n"
            + "root.wind1.azq01.speed: {ELB_INDEX=[]}>\n",
        newRouter.toString());

    // delete index
    newRouter.removeIndexFromRouter(new PartialPath(index_full), NO_INDEX);
    Assert.assertEquals(
        "<{RTREE_PAA=[type: RTREE_PAA, time: 5, props: {PAA_DIM=5}]}\n"
            + "root.wind2.*.direction: {RTREE_PAA=nMax:50,nMin:2,dim:4,seedsPicker:LINEARinner:0,leaf:1,item:0;\n"
            + " calc size: 52=4*3+2+(#inner(0)+#leaf(1)) * (2*dim(4)*4+2+4) + #item(0) * (dim(4)*4 + 2 + 8)}>\n"
            + "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {INDEX_SLIDE_STEP=8, INDEX_WINDOW_RANGE=4}]}\n"
            + "root.wind1.azq01.speed: {ELB_INDEX=[]}>\n",
        newRouter.toString());
    newRouter.removeIndexFromRouter(new PartialPath(index_full), RTREE_PAA);
    Assert.assertEquals(
        "<{ELB_INDEX=[type: ELB_INDEX, time: 0, props: {INDEX_SLIDE_STEP=8, INDEX_WINDOW_RANGE=4}]}\n"
            + "root.wind1.azq01.speed: {ELB_INDEX=[]}>\n",
        newRouter.toString());

    newRouter.removeIndexFromRouter(new PartialPath(index_sub), ELB_INDEX);
    Assert.assertEquals("", newRouter.toString());
  }
}
