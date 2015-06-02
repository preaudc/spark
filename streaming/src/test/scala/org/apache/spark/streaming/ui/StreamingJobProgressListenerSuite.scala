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

package org.apache.spark.streaming.ui

<<<<<<< HEAD
import org.scalatest.Matchers

=======
import java.util.Properties

import org.scalatest.Matchers

import org.apache.spark.scheduler.SparkListenerJobStart
>>>>>>> upstream/master
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.{Duration, Time, Milliseconds, TestSuiteBase}

class StreamingJobProgressListenerSuite extends TestSuiteBase with Matchers {

  val input = (1 to 4).map(Seq(_)).toSeq
  val operation = (d: DStream[Int]) => d.map(x => x)

<<<<<<< HEAD
=======
  private def createJobStart(
      batchTime: Time, outputOpId: Int, jobId: Int): SparkListenerJobStart = {
    val properties = new Properties()
    properties.setProperty(JobScheduler.BATCH_TIME_PROPERTY_KEY, batchTime.milliseconds.toString)
    properties.setProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY, outputOpId.toString)
    SparkListenerJobStart(jobId = jobId,
      0L, // unused
      Nil, // unused
      properties)
  }

>>>>>>> upstream/master
  override def batchDuration: Duration = Milliseconds(100)

  test("onBatchSubmitted, onBatchStarted, onBatchCompleted, " +
    "onReceiverStarted, onReceiverError, onReceiverStopped") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

<<<<<<< HEAD
    val receivedBlockInfo = Map(
      0 -> Array(ReceivedBlockInfo(0, 100, null), ReceivedBlockInfo(0, 200, null)),
      1 -> Array(ReceivedBlockInfo(1, 300, null))
    )

    // onBatchSubmitted
    val batchInfoSubmitted = BatchInfo(Time(1000), receivedBlockInfo, 1000, None, None)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))
    listener.waitingBatches should be (List(batchInfoSubmitted))
=======
    val streamIdToNumRecords = Map(0 -> 300L, 1 -> 300L)

    // onBatchSubmitted
    val batchInfoSubmitted = BatchInfo(Time(1000), streamIdToNumRecords, 1000, None, None)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))
    listener.waitingBatches should be (List(BatchUIData(batchInfoSubmitted)))
>>>>>>> upstream/master
    listener.runningBatches should be (Nil)
    listener.retainedCompletedBatches should be (Nil)
    listener.lastCompletedBatch should be (None)
    listener.numUnprocessedBatches should be (1)
    listener.numTotalCompletedBatches should be (0)
    listener.numTotalProcessedRecords should be (0)
    listener.numTotalReceivedRecords should be (0)

    // onBatchStarted
<<<<<<< HEAD
    val batchInfoStarted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)
    listener.onBatchStarted(StreamingListenerBatchStarted(batchInfoStarted))
    listener.waitingBatches should be (Nil)
    listener.runningBatches should be (List(batchInfoStarted))
=======
    val batchInfoStarted = BatchInfo(Time(1000), streamIdToNumRecords, 1000, Some(2000), None)
    listener.onBatchStarted(StreamingListenerBatchStarted(batchInfoStarted))
    listener.waitingBatches should be (Nil)
    listener.runningBatches should be (List(BatchUIData(batchInfoStarted)))
>>>>>>> upstream/master
    listener.retainedCompletedBatches should be (Nil)
    listener.lastCompletedBatch should be (None)
    listener.numUnprocessedBatches should be (1)
    listener.numTotalCompletedBatches should be (0)
    listener.numTotalProcessedRecords should be (0)
    listener.numTotalReceivedRecords should be (600)

<<<<<<< HEAD
    // onBatchCompleted
    val batchInfoCompleted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)
    listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    listener.waitingBatches should be (Nil)
    listener.runningBatches should be (Nil)
    listener.retainedCompletedBatches should be (List(batchInfoCompleted))
    listener.lastCompletedBatch should be (Some(batchInfoCompleted))
=======
    // onJobStart
    val jobStart1 = createJobStart(Time(1000), outputOpId = 0, jobId = 0)
    listener.onJobStart(jobStart1)

    val jobStart2 = createJobStart(Time(1000), outputOpId = 0, jobId = 1)
    listener.onJobStart(jobStart2)

    val jobStart3 = createJobStart(Time(1000), outputOpId = 1, jobId = 0)
    listener.onJobStart(jobStart3)

    val jobStart4 = createJobStart(Time(1000), outputOpId = 1, jobId = 1)
    listener.onJobStart(jobStart4)

    val batchUIData = listener.getBatchUIData(Time(1000))
    batchUIData should not be None
    batchUIData.get.batchTime should be (batchInfoStarted.batchTime)
    batchUIData.get.schedulingDelay should be (batchInfoStarted.schedulingDelay)
    batchUIData.get.processingDelay should be (batchInfoStarted.processingDelay)
    batchUIData.get.totalDelay should be (batchInfoStarted.totalDelay)
    batchUIData.get.streamIdToNumRecords should be (Map(0 -> 300L, 1 -> 300L))
    batchUIData.get.numRecords should be(600)
    batchUIData.get.outputOpIdSparkJobIdPairs should be
      Seq(OutputOpIdAndSparkJobId(0, 0),
        OutputOpIdAndSparkJobId(0, 1),
        OutputOpIdAndSparkJobId(1, 0),
        OutputOpIdAndSparkJobId(1, 1))

    // onBatchCompleted
    val batchInfoCompleted = BatchInfo(Time(1000), streamIdToNumRecords, 1000, Some(2000), None)
    listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    listener.waitingBatches should be (Nil)
    listener.runningBatches should be (Nil)
    listener.retainedCompletedBatches should be (List(BatchUIData(batchInfoCompleted)))
    listener.lastCompletedBatch should be (Some(BatchUIData(batchInfoCompleted)))
>>>>>>> upstream/master
    listener.numUnprocessedBatches should be (0)
    listener.numTotalCompletedBatches should be (1)
    listener.numTotalProcessedRecords should be (600)
    listener.numTotalReceivedRecords should be (600)

    // onReceiverStarted
    val receiverInfoStarted = ReceiverInfo(0, "test", null, true, "localhost")
    listener.onReceiverStarted(StreamingListenerReceiverStarted(receiverInfoStarted))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (None)

    // onReceiverError
    val receiverInfoError = ReceiverInfo(1, "test", null, true, "localhost")
    listener.onReceiverError(StreamingListenerReceiverError(receiverInfoError))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (Some(receiverInfoError))
    listener.receiverInfo(2) should be (None)

    // onReceiverStopped
    val receiverInfoStopped = ReceiverInfo(2, "test", null, true, "localhost")
    listener.onReceiverStopped(StreamingListenerReceiverStopped(receiverInfoStopped))
    listener.receiverInfo(0) should be (Some(receiverInfoStarted))
    listener.receiverInfo(1) should be (Some(receiverInfoError))
    listener.receiverInfo(2) should be (Some(receiverInfoStopped))
    listener.receiverInfo(3) should be (None)
  }

  test("Remove the old completed batches when exceeding the limit") {
    val ssc = setupStreams(input, operation)
<<<<<<< HEAD
    val limit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 100)
    val listener = new StreamingJobProgressListener(ssc)

    val receivedBlockInfo = Map(
      0 -> Array(ReceivedBlockInfo(0, 100, null), ReceivedBlockInfo(0, 200, null)),
      1 -> Array(ReceivedBlockInfo(1, 300, null))
    )
    val batchInfoCompleted = BatchInfo(Time(1000), receivedBlockInfo, 1000, Some(2000), None)
=======
    val limit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)
    val listener = new StreamingJobProgressListener(ssc)

    val streamIdToNumRecords = Map(0 -> 300L, 1 -> 300L)

    val batchInfoCompleted = BatchInfo(Time(1000), streamIdToNumRecords, 1000, Some(2000), None)
>>>>>>> upstream/master

    for(_ <- 0 until (limit + 10)) {
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    }

    listener.retainedCompletedBatches.size should be (limit)
    listener.numTotalCompletedBatches should be(limit + 10)
  }
<<<<<<< HEAD
=======

  test("out-of-order onJobStart and onBatchXXX") {
    val ssc = setupStreams(input, operation)
    val limit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)
    val listener = new StreamingJobProgressListener(ssc)

    // fulfill completedBatchInfos
    for(i <- 0 until limit) {
      val batchInfoCompleted =
        BatchInfo(Time(1000 + i * 100), Map.empty, 1000 + i * 100, Some(2000 + i * 100), None)
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
      val jobStart = createJobStart(Time(1000 + i * 100), outputOpId = 0, jobId = 1)
      listener.onJobStart(jobStart)
    }

    // onJobStart happens before onBatchSubmitted
    val jobStart = createJobStart(Time(1000 + limit * 100), outputOpId = 0, jobId = 0)
    listener.onJobStart(jobStart)

    val batchInfoSubmitted =
      BatchInfo(Time(1000 + limit * 100), Map.empty, (1000 + limit * 100), None, None)
    listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))

    // We still can see the info retrieved from onJobStart
    val batchUIData = listener.getBatchUIData(Time(1000 + limit * 100))
    batchUIData should not be None
    batchUIData.get.batchTime should be (batchInfoSubmitted.batchTime)
    batchUIData.get.schedulingDelay should be (batchInfoSubmitted.schedulingDelay)
    batchUIData.get.processingDelay should be (batchInfoSubmitted.processingDelay)
    batchUIData.get.totalDelay should be (batchInfoSubmitted.totalDelay)
    batchUIData.get.streamIdToNumRecords should be (Map.empty)
    batchUIData.get.numRecords should be (0)
    batchUIData.get.outputOpIdSparkJobIdPairs should be (Seq(OutputOpIdAndSparkJobId(0, 0)))

    // A lot of "onBatchCompleted"s happen before "onJobStart"
    for(i <- limit + 1 to limit * 2) {
      val batchInfoCompleted =
        BatchInfo(Time(1000 + i * 100), Map.empty, 1000 + i * 100, Some(2000 + i * 100), None)
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    }

    for(i <- limit + 1 to limit * 2) {
      val jobStart = createJobStart(Time(1000 + i * 100), outputOpId = 0, jobId = 1)
      listener.onJobStart(jobStart)
    }

    // We should not leak memory
    listener.batchTimeToOutputOpIdSparkJobIdPair.size() should be <=
      (listener.waitingBatches.size + listener.runningBatches.size +
        listener.retainedCompletedBatches.size + 10)
  }

  test("detect memory leak") {
    val ssc = setupStreams(input, operation)
    val listener = new StreamingJobProgressListener(ssc)

    val limit = ssc.conf.getInt("spark.streaming.ui.retainedBatches", 1000)

    for (_ <- 0 until 2 * limit) {
      val streamIdToNumRecords = Map(0 -> 300L, 1 -> 300L)

      // onBatchSubmitted
      val batchInfoSubmitted = BatchInfo(Time(1000), streamIdToNumRecords, 1000, None, None)
      listener.onBatchSubmitted(StreamingListenerBatchSubmitted(batchInfoSubmitted))

      // onBatchStarted
      val batchInfoStarted = BatchInfo(Time(1000), streamIdToNumRecords, 1000, Some(2000), None)
      listener.onBatchStarted(StreamingListenerBatchStarted(batchInfoStarted))

      // onJobStart
      val jobStart1 = createJobStart(Time(1000), outputOpId = 0, jobId = 0)
      listener.onJobStart(jobStart1)

      val jobStart2 = createJobStart(Time(1000), outputOpId = 0, jobId = 1)
      listener.onJobStart(jobStart2)

      val jobStart3 = createJobStart(Time(1000), outputOpId = 1, jobId = 0)
      listener.onJobStart(jobStart3)

      val jobStart4 = createJobStart(Time(1000), outputOpId = 1, jobId = 1)
      listener.onJobStart(jobStart4)

      // onBatchCompleted
      val batchInfoCompleted = BatchInfo(Time(1000), streamIdToNumRecords, 1000, Some(2000), None)
      listener.onBatchCompleted(StreamingListenerBatchCompleted(batchInfoCompleted))
    }

    listener.waitingBatches.size should be (0)
    listener.runningBatches.size should be (0)
    listener.retainedCompletedBatches.size should be (limit)
    listener.batchTimeToOutputOpIdSparkJobIdPair.size() should be <=
      (listener.waitingBatches.size + listener.runningBatches.size +
        listener.retainedCompletedBatches.size + 10)
  }

>>>>>>> upstream/master
}
