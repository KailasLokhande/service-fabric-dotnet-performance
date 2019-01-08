// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace ServiceLoadTestClient
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Fabric;
    using System.Fabric.Query;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography.X509Certificates;
    using System.ServiceModel;
    using System.Threading;
    using System.Threading.Tasks;
    using LoadDriverLib;
    using LoadDriverServiceInterface;
    using Microsoft.Azure.CosmosDB.Table;
    using Microsoft.Azure.Storage;
    using ServiceLoadTestUtilities;

    internal class Program
    {
        private const string StandardServiceNamePrefix = "fabric:/";
        private const string ReverseProxyUriTemplate = "http://{0}{1}?PartitionKey={2}&PartitionKind=Int64Range&Timeout={3}";
        private const int TimeoutInSeconds = 10800;
        private static readonly Uri LoadDriverServiceUri = new Uri("fabric:/LoadDriverApplication/LoadDriverService");

        private static void Main(string[] args)
        {
            MainAsync(args).Wait();
            PrintResult();
        }

        private static void PrintResult()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=kailaslsaai;AccountKey=XjdcJGjs97UDCJTmlMehQxUk+b4sa/iqgeemGDapfVZi9+a6dXQSbWAjx0Qx/j8WrnvIQiDhgoWgFmfhxvK7LA==;EndpointSuffix=core.windows.net");

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            CloudTable operationsTable = tableClient.GetTableReference("Operations");

            if (!operationsTable.Exists())
                Console.WriteLine("ERROR!! Could not find results table");

            PrintResultsForOperation(operationsTable, "WriteOperation");
            PrintResultsForOperation(operationsTable, "ReadOperation");
        }

        private static void PrintResultsForOperation(CloudTable operationsTable, string operationName)
        {
            TableQuery<OperationsEntity> query = new TableQuery<OperationsEntity>().Where(TableQuery.GenerateFilterCondition("operationName", QueryComparisons.Equal, operationName));

            //operationsTable.ExecuteQuery(query).Count<OperationsEntity>();
            // Console.WriteLine();
            // Print the fields for each customer.
            List<OperationsEntity> operations = new List<OperationsEntity>();

            operations.AddRange(operationsTable.ExecuteQuery(query));
            Console.WriteLine("Total {0}", operations.Count);
            List<OperationsEntity> orderedList = operations.OrderBy(o => o.duration).ToList<OperationsEntity>();
            double percentile50 = (orderedList[(int)(operations.Count * 0.5)].duration)/10000;
            double percentile60 = (orderedList[(int)(operations.Count * 0.6)].duration)/ 10000;
            double percentile70 = (orderedList[(int)(operations.Count * 0.7)].duration)/ 10000;
            double percentile80 = (orderedList[(int)(operations.Count * 0.8)].duration)/ 10000;
            double percentile90 = (orderedList[(int)(operations.Count * 0.9)].duration)/ 10000;
            double percentile95 = (orderedList[(int)(operations.Count * 0.95)].duration)/ 10000;
            double percentile995 = (orderedList[(int)(operations.Count * 0.995)].duration)/ 10000;
            double percentile999 = (orderedList[(int)(operations.Count * 0.999)].duration)/ 10000;

            //List<OperationsEntity> orderedExecList = operations.OrderBy(o => o.executionDuration).ToList<OperationsEntity>();
            //double percentileExec50 = (orderedList[(int)(operations.Count * 0.5)].executionDuration) / 10000;
            //double percentileExec60 = (orderedList[(int)(operations.Count * 0.6)].executionDuration) / 10000;
            //double percentileExec70 = (orderedList[(int)(operations.Count * 0.7)].executionDuration) / 10000;
            //double percentileExec80 = (orderedList[(int)(operations.Count * 0.8)].executionDuration) / 10000;
            //double percentileExec90 = (orderedList[(int)(operations.Count * 0.9)].executionDuration) / 10000;
            //double percentileExec95 = (orderedList[(int)(operations.Count * 0.95)].executionDuration) / 10000;
            //double percentileExec995 = (orderedList[(int)(operations.Count * 0.995)].executionDuration) / 10000;
            //double percentileExec999 = (orderedList[(int)(operations.Count * 0.999)].executionDuration) / 10000;
            string path = @"F:\NativePersisted" + Guid.NewGuid().ToString("N") + "--" + operationName + ".txt";
            using (StreamWriter sw = File.CreateText(path))
            {
                sw.WriteLine($"Opertaion: {operationName} Total: {operations.Count} \n \n Percentile 50 => {percentile50} msec\nPercentile 60 => {percentile60} msec\nPercentile 70 => {percentile70} msec\nPercentile 80 => {percentile80} msec\nPercentile 90 => {percentile90} msec\nPercentile 95 => {percentile95} msec\nPercentile 99.5 => {percentile995} msec\nPercentile 99.9 => {percentile999} msec");
                sw.Flush();
            }

            Console.WriteLine($"Opertaion: {operationName} Total: {operations.Count} \n \n Percentile 50 => {percentile50} msec\nPercentile 60 => {percentile60} msec\nPercentile 70 => {percentile70} msec\nPercentile 80 => {percentile80} msec\nPercentile 90 => {percentile90} msec\nPercentile 95 => {percentile95} msec\nPercentile 99.5 => {percentile995} msec\nPercentile 99.9 => {percentile999} msec");
            //Console.WriteLine($"\n\nExec Opertaion: {operationName} Total: {operations.Count} \n \n Percentile 50 => {percentileExec50} msec\nPercentile 60 => {percentileExec60} msec\nPercentile 70 => {percentileExec70} msec\nPercentile 80 => {percentileExec80} msec\nPercentile 90 => {percentileExec90} msec\nPercentile 95 => {percentileExec95} msec\nPercentile 99.5 => {percentileExec995} msec\nPercentile 99.9 => {percentileExec999} msec");

        }

        private static async Task MainAsync(string[] args)
        {

            // Read the parameters from the configuration file and command line
            Parameters parameters = new Parameters();
            parameters.ReadFromConfigFile();
            parameters.OverrideFromCommandLine(args);

            Console.WriteLine("Running test against {0}",
                    (string)parameters.ParameterValues[Parameters.Id.ClusterAddress]);

            // Create the test specifications for each client
            int numClients = (int)parameters.ParameterValues[Parameters.Id.NumClients];
            TestSpecifications[] testSpecifications = CreateTestSpecifications(parameters, numClients);

            // Wait until the load driver service (that hosts the clients) is ready
            X509Credentials credentials = new X509Credentials()
            {
                StoreLocation = StoreLocation.CurrentUser,
                StoreName = new X509Store(StoreName.My, StoreLocation.CurrentUser).Name,
                FindType = X509FindType.FindByThumbprint,
                FindValue = (string)parameters.ParameterValues[Parameters.Id.ClientCertificateThumbprint],
            };
            credentials.RemoteCertThumbprints.Add(
                (string)parameters.ParameterValues[Parameters.Id.ServerCertificateThumbprint]);

            FabricClient fabricClient = new FabricClient(
                credentials,
                new FabricClientSettings(),
                GetEndpointAddress(
                    (string)parameters.ParameterValues[Parameters.Id.ClusterAddress],
                    (int)parameters.ParameterValues[Parameters.Id.ClientConnectionPort]));
            ServicePartitionList partitionList = await AwaitPartitionReadyOperation.PerformAsync(
                fabricClient,
                LoadDriverServiceUri);

            // Verify that the load driver service has at least as many partitions as the number of
            // clients that we need to create.
            if (partitionList.Count < numClients)
            {
                string message = String.Format(
                    "The value for parameter '{0}' ({1}) should not be greater than the number of partitions ({2}) of the '{3}' service.",
                    Parameters.ParameterNames.Single(kvp => (kvp.Value == Parameters.Id.NumClients)).Key,
                    numClients,
                    partitionList.Count,
                    LoadDriverServiceUri.AbsoluteUri);
                throw new ConfigurationErrorsException(message);
            }

            // Get the interfaces for each instance of the load driver service.
            ILoadDriver[] loadDrivers = CreateLoadDrivers(
                GetEndpointAddress(
                    (string)parameters.ParameterValues[Parameters.Id.ClusterAddress],
                    (int)parameters.ParameterValues[Parameters.Id.ReverseProxyPort]),
                partitionList);

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=kailaslsaai;AccountKey=XjdcJGjs97UDCJTmlMehQxUk+b4sa/iqgeemGDapfVZi9+a6dXQSbWAjx0Qx/j8WrnvIQiDhgoWgFmfhxvK7LA==;EndpointSuffix=core.windows.net");

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            CloudTable operationsTable = tableClient.GetTableReference("Operations");

            // Create the table if it doesn't exist.
            operationsTable.DeleteIfExists();

            await Task.Delay(TimeSpan.FromMinutes(2));
            // Create and initialize the clients inside the load driver service.
            Task[] initializationTasks = new Task[numClients];
            for (int i = 0; i < numClients; i++)
            {
                initializationTasks[i] = loadDrivers[i].InitializeAsync(testSpecifications[i]);
            }
            await Task.WhenAll(initializationTasks);

            TestResults writeTestResults = null;
            try
            {
                Console.WriteLine("Starting write test...");
                // Run the tests
                writeTestResults = await RunTestAsync(
                    numClients,
                    loadDrivers,
                    ld => ld.RunWriteTestAsync());
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(15));
                await WaitForLockAsync("write", writeTestResults.lockfile);
                Console.WriteLine("Write test finished");

                await Task.Delay(1000 * 60);
                //     Console.Write("Press enter to start read test...");
                //     Console.ReadLine();

                Console.WriteLine("Starting read test...");
                TestResults readTestResults = await RunTestAsync(
                    numClients,
                    loadDrivers,
                    ld => ld.RunReadTestAsync());

                await Task.Delay(TimeSpan.FromSeconds(15));
                await WaitForLockAsync("read", readTestResults.lockfile);

                // Display the results
                Console.WriteLine("Read test finished");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static async Task WaitForLockAsync(string testname, System.Collections.Generic.List<string> lockfile)
        {
            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=kailaslsaai;AccountKey=BiND7baqFuk+oV1xaJKHZOtM8Em9kVUHU5vqVWFZuKyofnmD/nt9wjcOECys26JMoOLcyvCYGMrOq7xICp5Mng==;EndpointSuffix=core.windows.net";
            Microsoft.WindowsAzure.Storage.CloudStorageAccount account;
            Microsoft.WindowsAzure.Storage.CloudStorageAccount.TryParse(storageConnectionString, out account);

            while (lockfile.Count > 0)
            {
                var client = account.CreateCloudBlobClient();
                var container = client.GetContainerReference("lock");
                container.CreateIfNotExists();
                string lockfileName = lockfile[0];
                var blob = container.GetBlockBlobReference(lockfileName);
                while (await blob.ExistsAsync())
                {
                    Console.WriteLine("{0} - Waiting for {1} test to finish... Lock check {2}", DateTime.Now, testname, lockfileName);
                    await Task.Delay(TimeSpan.FromMinutes(1));
                }
                lockfile.Remove(lockfileName);
            }
        }

        private static async Task<TestResults> RunTestAsync(
            int numClients,
            ILoadDriver[] loadDrivers,
            Func<ILoadDriver, Task<TestResults>> runTestOnSingleDriverInstance)
        {
            Task<TestResults>[] testTasks = new Task<TestResults>[numClients];

            // Trigger the test run for each of the clients and wait for them all
            // to finish. Also measure how to long it took for all of them to
            // finish running their tests.
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            for (int i = 0; i < numClients; i++)
            {
                testTasks[i] = runTestOnSingleDriverInstance(loadDrivers[i]);
            }

            try
            {
                await Task.WhenAll(testTasks);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }


            stopwatch.Stop();

            // Merge the raw results from all of the clients
            TestResults results = testTasks[0].Result;
            for (int i = 1; i < numClients; i++)
            {
                results = TestResults.Combine(results, testTasks[i].Result);
            }

            // Compute averages based on the raw results from the clients.
            results.ComputeAverages(stopwatch.Elapsed);
            return results;
        }

        private static ILoadDriver[] CreateLoadDrivers(string reverseProxyAddress, ServicePartitionList partitionList)
        {
            ILoadDriver[] loadDrivers = new ILoadDriver[partitionList.Count];
            int i = 0;
            foreach (Partition partition in partitionList)
            {
                // We use the reverse proxy to communicate with each partition of the
                // load driver service (which hosts the clients).
                string uri = GetUriExposedByReverseProxy(
                    LoadDriverServiceUri.AbsoluteUri,
                    partition,
                    reverseProxyAddress);
                loadDrivers[i] = ChannelFactory<ILoadDriver>.CreateChannel(
                    BindingUtility.CreateHttpBinding(),
                    new EndpointAddress(uri));
                i++;
            }
            return loadDrivers;
        }

        private static string GetUriExposedByReverseProxy(string serviceName, Partition partition, string reverseProxyAddress)
        {
            string serviceNameSuffix = serviceName.Remove(0, StandardServiceNamePrefix.Length);
            return String.Format(
                ReverseProxyUriTemplate,
                String.Concat(
                    reverseProxyAddress,
                    reverseProxyAddress.EndsWith("/") ? String.Empty : "/"),
                serviceNameSuffix,
                ((Int64RangePartitionInformation)partition.PartitionInformation).LowKey,
                TimeoutInSeconds.ToString("D"));
        }

        private static string GetEndpointAddress(string clusterAddress, int port)
        {
            return String.Format("{0}:{1}", clusterAddress, port);
        }

        private static TestSpecifications[] CreateTestSpecifications(Parameters parameters, int numClients)
        {
            TargetService.Description targetServiceType =
                TargetService.SupportedTypes[(TargetService.Types)parameters.ParameterValues[Parameters.Id.TargetServiceType]];
            TestSpecifications[] testSpecifications = new TestSpecifications[numClients];

            // Distribute the total work among the clients
            int numWriteOperationsTotal = (int)parameters.ParameterValues[Parameters.Id.NumWriteOperationsTotal];
            int numWriteOperationsPerClient = numWriteOperationsTotal / numClients;
            int numWriteOperationsRemainder = numWriteOperationsTotal % numClients;

            int numOutstandingWriteOperations = (int)parameters.ParameterValues[Parameters.Id.NumOutstandingWriteOperations];
            int numOutstandingWriteOperationsPerClient = numOutstandingWriteOperations / numClients;
            int numOutstandingWriteOperationsRemainder = numOutstandingWriteOperations % numClients;

            int numReadOperationsTotal = (int)parameters.ParameterValues[Parameters.Id.NumReadOperationsTotal];
            int numReadOperationsPerClient = numReadOperationsTotal / numClients;
            int numReadOperationsRemainder = numReadOperationsTotal % numClients;

            int numOutstandingReadOperations = (int)parameters.ParameterValues[Parameters.Id.NumOutstandingReadOperations];
            int numOutstandingReadOperationsPerClient = numOutstandingReadOperations / numClients;
            int numOutstandingReadOperationsRemainder = numOutstandingReadOperations % numClients;

            int numItems = (int)parameters.ParameterValues[Parameters.Id.NumItems];
            int numItemsPerClient = numItems / numClients;
            int numItemsRemainder = numItems % numClients;

            for (int i = 0; i < numClients; i++)
            {
                // Create test specification for client
                testSpecifications[i] = new TestSpecifications()
                {
                    NumWriteOperationsTotal = numWriteOperationsPerClient,
                    NumOutstandingWriteOperations = numOutstandingWriteOperationsPerClient,
                    NumReadOperationsTotal = numReadOperationsPerClient,
                    NumOutstandingReadOperations = numOutstandingReadOperationsPerClient,
                    OperationDataSizeInBytes = (int)parameters.ParameterValues[Parameters.Id.OperationDataSizeInBytes],
                    NumItems = numItemsPerClient,
                    RequestSenderAssemblyName = targetServiceType.AssemblyName,
                    RequestSenderTypeName = targetServiceType.TypeName
                };

                if (i < numWriteOperationsRemainder)
                {
                    (testSpecifications[i].NumWriteOperationsTotal)++;
                }
                if (i < numOutstandingWriteOperationsRemainder)
                {
                    (testSpecifications[i].NumOutstandingWriteOperations)++;
                }
                if (i < numReadOperationsRemainder)
                {
                    (testSpecifications[i].NumReadOperationsTotal)++;
                }
                if (i < numOutstandingReadOperationsRemainder)
                {
                    (testSpecifications[i].NumOutstandingReadOperations)++;
                }
                if (i < numItemsRemainder)
                {
                    (testSpecifications[i].NumItems)++;
                }
            }

            return testSpecifications;
        }
    }
}