// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace LoadDriverLib
{
    using System;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using RequestSenderInterface;

    /// <summary>
    /// This class represents a client that generates load on the target service.
    /// </summary>
    public class TestExecutor
    {
        private int state;
        private TestSpecifications specifications;
        private IRequestSender requestSender;
        private string uniqueIdentifier = "";

        public async Task InitializeAsync(TestSpecifications testSpecifications)
        {
            this.DoVerifiedStateTransition(TestExecutorState.Uninitialized, TestExecutorState.Initializing);

            this.specifications = testSpecifications;
            this.requestSender = await this.GetRequestSenderAsync();

            this.DoStateTransition(TestExecutorState.Initialized);
            this.uniqueIdentifier = Guid.NewGuid().ToString("N") ;
        }

        public async Task AcquireDistributedLock(string owner)
        {
            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=kailaslsaai;AccountKey=BiND7baqFuk+oV1xaJKHZOtM8Em9kVUHU5vqVWFZuKyofnmD/nt9wjcOECys26JMoOLcyvCYGMrOq7xICp5Mng==;EndpointSuffix=core.windows.net";
            CloudStorageAccount account;
            CloudStorageAccount.TryParse(storageConnectionString, out account);

            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference("lock");
            container.CreateIfNotExists();
            var blob = container.GetBlockBlobReference("lockfile" + "--" + uniqueIdentifier);
            await blob.UploadTextAsync(owner);
        }

        public async Task ReleaseDistributedLock(string owner)
        {
            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=kailaslsaai;AccountKey=BiND7baqFuk+oV1xaJKHZOtM8Em9kVUHU5vqVWFZuKyofnmD/nt9wjcOECys26JMoOLcyvCYGMrOq7xICp5Mng==;EndpointSuffix=core.windows.net";
            CloudStorageAccount account;
            CloudStorageAccount.TryParse(storageConnectionString, out account);

            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference("lock");
            container.CreateIfNotExists();
            var blob = container.GetBlockBlobReference("lockfile" + "--" + uniqueIdentifier);
            await blob.DeleteAsync();
        }

        public async Task UploadTestResults(string type, TestResults results)
        {
            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=kailaslsaai;AccountKey=BiND7baqFuk+oV1xaJKHZOtM8Em9kVUHU5vqVWFZuKyofnmD/nt9wjcOECys26JMoOLcyvCYGMrOq7xICp5Mng==;EndpointSuffix=core.windows.net";
            CloudStorageAccount account;
            CloudStorageAccount.TryParse(storageConnectionString, out account);

            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(type + "results");
            container.CreateIfNotExists();
            var blob = container.GetBlockBlobReference(DateTime.Now.ToString("yyyyMMdd-HHmmss" + "--" + uniqueIdentifier));
            await blob.UploadTextAsync(results.ToString());
        }


        public async Task<TestResults> RunWriteTestAsync()
        {
            await AcquireDistributedLock("writer");
#pragma warning disable 4014
            Task.Run(async () =>
            {
                try
                {
                    this.DoVerifiedStateTransition(TestExecutorState.Initialized, TestExecutorState.RunningWritePhase);

                    TestResults results = await this.RunTestAsync(
                        "WriteOperation",
                    (index) => this.requestSender.SendWriteRequestAsync(index),
                    this.specifications.NumWriteOperationsTotal,
                    this.specifications.NumOutstandingWriteOperations);
                    await UploadTestResults("write", results);

                    this.DoStateTransition(TestExecutorState.WritePhaseCompleted);
                }
                finally
                {
                    await ReleaseDistributedLock("writer");
                }
            });
#pragma warning restore 4014

            TestResults result = new TestResults();
            result.lockfile.Add("lockfile" + "--" + uniqueIdentifier);
            return result;
        }

        public async Task<TestResults> RunReadTestAsync()
        {
            await AcquireDistributedLock("reader");
#pragma warning disable 4014
            Task.Run(async () =>
            {
                try
                {
                    this.DoVerifiedStateTransition(TestExecutorState.WritePhaseCompleted, TestExecutorState.RunningReadPhase);

                    TestResults results = await this.RunTestAsync(
                        "ReadOperation",
                        (index) => this.requestSender.SendReadRequestAsync(index),
                        this.specifications.NumReadOperationsTotal,
                        this.specifications.NumOutstandingReadOperations);
                    await UploadTestResults("read", results);
                    this.DoStateTransition(TestExecutorState.ReadPhaseCompleted);

                }
                finally
                {
                    await ReleaseDistributedLock("reader");
                }
            });
#pragma warning restore 4014

            TestResults result = new TestResults();
            result.lockfile.Add("lockfile" + "--" + uniqueIdentifier);
            return result;
        }

        private async Task<IRequestSender> GetRequestSenderAsync()
        {
            // Create request sender
            Assembly assembly = Assembly.Load(this.specifications.RequestSenderAssemblyName);
            Type type = assembly.GetType(this.specifications.RequestSenderTypeName);
            IRequestSender sender = (IRequestSender) Activator.CreateInstance(type);
            RequestSenderSpecifications requestSenderSpecifications = new RequestSenderSpecifications()
            {
                NumItems = this.specifications.NumItems,
                OperationDataSizeInBytes = this.specifications.OperationDataSizeInBytes
            };

            // Initialize request sender
            await sender.InitializeAsync(requestSenderSpecifications);
            return sender;
        }

        private async Task<TestResults> RunTestAsync(
            string operationName,
            Func<int, Task> doTestOperationAsync,
            int numOperationsTotal,
            int numOutstandingOperations)
        {
            // Perfom operations on the service with the desired concurrency.
            ConcurrentOperationsRunner concurrentOpsRunner = new ConcurrentOperationsRunner(
                operationName,
                doTestOperationAsync,
                numOperationsTotal,
                numOutstandingOperations,
                this.specifications.NumItems);
            return await concurrentOpsRunner.RunAll();
        }

        private void DoStateTransition(TestExecutorState newState)
        {
            // Move the test to the desired state.
            Interlocked.Exchange(ref this.state, (int) newState);
        }

        private void DoVerifiedStateTransition(TestExecutorState expectedCurrentState, TestExecutorState newState)
        {
            // Move the test to the desired state after verifying that the move is valid. 
            int currentState = Interlocked.CompareExchange(
                ref this.state,
                (int) expectedCurrentState,
                (int) newState);
            if ((int) expectedCurrentState != currentState)
            {
                string message = String.Format(
                    "Test executor cannot move from current state {0} to new state {1}. In order to move, the current state must be {2}.",
                    currentState,
                    newState,
                    expectedCurrentState);
                throw new InvalidOperationException(message);
            }
        }
    }
}