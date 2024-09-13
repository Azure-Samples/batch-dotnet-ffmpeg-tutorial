// BatchDotnetTutorialFfmpeg is a .NET Framework console app project using Batch SDK for .NET.
// Demonstrates a basic Batch pool that runs ffmpeg tasks to transcode media files.

namespace BatchDotnetTutorialFfmpeg
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Drawing;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Compute.Batch;
    using Azure.Identity;
    using Azure.Storage;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Sas;
    using Azure.ResourceManager.Batch;
    using Azure.ResourceManager;
    using Azure.Core;
    using Azure.ResourceManager.Batch.Models;
    using Azure.ResourceManager.Models;
    using System.Threading;

    public class Program
    {

        // Pool and Job constants
        private const string PoolId = "WinFFmpegPool";
        private const int DedicatedNodeCount = 0;
        private const int LowPriorityNodeCount = 5;
        private const string PoolVMSize = "STANDARD_A1_v2";
        private const string JobId = "WinFFmpegJob";
        // TODO: Replace <Managed-Identiy-Resource-ID> with your actual managed account Resource Id
        private const string ManagedIdentityId = "<Managed-Identiy-Resource-ID>";
        // TODO: Replace <Batch-Account-Resource-ID> with your actual batch account Resource Id
        private const string BatchAccountResourceID = "<Batch-Account-Resource-ID>";

        // Application package Id and version
        // This assumes the Windows ffmpeg app package is already added to the Batch account with this Id and version. 
        // First download ffmpeg zipfile from https://ffmpeg.zeranoe.com/builds/win64/static/ffmpeg-3.4-win64-static.zip.
        // To add package to the Batch account, see https://docs.microsoft.com/azure/batch/batch-application-packages.
        const string appPackageId = "ffmpeg";
        // TODO: Replace <Subscription-ID>, <Resource-Group-ID>, <Batch-Account-Name> with your values.
        const string appPacakgeResourceID = "/subscriptions/<Subscription-ID>/resourceGroups/<Resource-Group-ID>/providers/Microsoft.Batch/batchAccounts/<Batch-Account-Name>/applications/ffmpeg";
        const string appPackageVersion = "4.3.1";

        public static void Main(string[] args)
        {
           
            try
            {
                // Call the asynchronous version of the Main() method. This is done so that we can await various
                // calls to async methods within the "Main" method of this console application.
                MainAsync().Wait();
            }
            catch (AggregateException)
            {
                Console.WriteLine();
                Console.WriteLine("One or more exceptions occurred.");
                Console.WriteLine();
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete, hit ENTER to exit...");
                Console.ReadLine();
            }
        }

        /// <summary>
        /// Provides an asynchronous version of the Main method, allowing for the awaiting of async method calls within.
        /// </summary>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private static async Task MainAsync()
        {
            Console.WriteLine("Sample start: {0}", DateTime.Now);
            Console.WriteLine();
            Stopwatch timer = new Stopwatch();
            timer.Start();


            // TODO: Replace <storage-account-name> with your actual storage account name
            Uri accountUri = new Uri("https://<storage-account-name>.blob.core.windows.net/");
            
            BlobServiceClient blobServerClient = new BlobServiceClient(accountUri, new DefaultAzureCredential());

            // Use the blob client to create the containers in blob storage
            const string inputContainerName = "input";
            const string outputContainerName = "output";

            CreateContainerIfNotExist(blobServerClient, inputContainerName);
            CreateContainerIfNotExist(blobServerClient, outputContainerName);
            
            // RESOURCE FILE SETUP
            // Input files: Specify the location of the data files that the tasks process, and
            // put them in a List collection. Make sure you have copied the data files to:
            // \<solutiondir>\InputFiles.

            string inputPath = Path.Combine(Environment.CurrentDirectory, "InputFiles");

            List<string> inputFilePaths = new List<string>(Directory.GetFileSystemEntries(inputPath, "*.mp4",
                                         SearchOption.TopDirectoryOnly));
            
            // Upload data files.
            // Upload the data files using UploadResourceFilesToContainer(). This data will be
            // processed by each of the tasks that are executed on the compute nodes within the pool.
            List<ResourceFile> inputFiles = await UploadFilesToContainerAsync(blobServerClient, inputContainerName, inputFilePaths);


            // CREATE BATCH CLIENT / CREATE POOL / CREATE JOB / ADD TASKS

            // Create a Batch client and authenticate with DefaultAzureCredential.
            // The Batch client allows the app to interact with the Batch service.

            // TODO: Replace <batch-account-name> with your actual batch account name
            Uri batchUri = new Uri("https://<batch-account-name>.eastus.batch.azure.com");
            BatchClient batchClient = new BatchClient(batchUri, new DefaultAzureCredential());


            // Create the Batch pool, which contains the compute nodes that execute the tasks.
            await CreatePoolIfNotExistAsync(PoolId);

                
            // Create the job that runs the tasks.
            await CreateJobAsync(batchClient, JobId, PoolId);

            
            // Create a collection of tasks and add them to the Batch job. 
            // Provide a shared access signature for the tasks so that they can upload their output
            // to the Storage container.
            await AddTasksAsync(batchClient, JobId, inputFiles, "");
            
            // Monitor task success or failure, specifying a maximum amount of time to wait for
            // the tasks to complete.
            await MonitorTasks(batchClient, JobId, TimeSpan.FromMinutes(30));
            
            // Delete input container in storage
            Console.WriteLine("Deleting container [{0}]...", inputContainerName);
            BlobContainerClient containerClient = blobServerClient.GetBlobContainerClient(inputContainerName);
            await containerClient.DeleteIfExistsAsync();
                   
            // Print out timing info
            timer.Stop();
            Console.WriteLine();
            Console.WriteLine("Sample end: {0}", DateTime.Now);
            Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

            // Clean up Batch resources (if the user so chooses)
            Console.WriteLine();
            Console.Write("Delete job? [yes] no: ");
            string response = Console.ReadLine().ToLower();
            if (response != "n" && response != "no")
            {
                   await batchClient.DeleteJobAsync(JobId);
            }

            Console.Write("Delete pool? [yes] no: ");
            response = Console.ReadLine().ToLower();
            if (response != "n" && response != "no")
            {
               await batchClient.DeletePoolAsync(PoolId);
            }
        }
       
        // FUNCTION IMPLEMENTATIONS

        /// <summary>
        /// Creates a container with the specified name in Blob storage, unless a container with that name already exists.
        /// </summary>
        /// <param name="blobServiceClient">A <see cref="BlobServiceClient"/>.</param>
        /// <param name="containerName">The name for the new container.</param>

        private static void CreateContainerIfNotExist(BlobServiceClient blobServiceClient, string containerName)
        {
            try
            {
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                containerClient.CreateIfNotExists();

                Console.WriteLine("Creating container [{0}].", containerName);
            }catch ( Exception e)
            {
                Console.WriteLine("Error creating container [{0}].", containerName);
                Console.WriteLine(e.Message);
            }
        }

        
        // RESOURCE FILE SETUP - FUNCTION IMPLEMENTATIONS

        /// <summary>
        /// Uploads the specified resource files to a container.
        /// </summary>
        /// <param name="blobClient">A <see cref="BlobServiceClient"/>.</param>
        /// <param name="containerName">Name of the blob storage container to which the files are uploaded.</param>
        /// <param name="filePaths">A collection of paths of the files to be uploaded to the container.</param>
        /// <returns>A collection of <see cref="ResourceFile"/> objects.</returns>
        private static async Task<List<ResourceFile>> UploadFilesToContainerAsync(BlobServiceClient blobClient, string inputContainerName, List<string> filePaths)
        {
            List<ResourceFile> resourceFiles = new List<ResourceFile>();

            foreach (string filePath in filePaths)
            {
                resourceFiles.Add(await UploadResourceFileToContainerAsync(blobClient, inputContainerName, filePath));
            }

            return resourceFiles;
        }

        /// <summary>
        /// Uploads the specified file to the specified blob container.
        /// </summary>
        /// <param name="blobServiceClient">A <see cref="BlobServiceClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <returns>A ResourceFile object representing the file in blob storage.</returns>
        private static async Task<ResourceFile> UploadResourceFileToContainerAsync(BlobServiceClient blobServiceClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);
            var fileStream = System.IO.File.OpenRead(filePath);

            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            BlobClient blobClient = containerClient.GetBlobClient(blobName);

            if (!blobClient.Exists())
            {
                await blobClient.UploadAsync(filePath);
            }

            return new ResourceFile() 
                {
                IdentityReference = new BatchNodeIdentityReference() { ResourceId= ManagedIdentityId },
                StorageContainerUrl = blobClient.Uri.AbsoluteUri, 
            };
        }


        // BATCH CLIENT OPERATIONS - FUNCTION IMPLEMENTATIONS

        /// <summary>
        /// Creates the Batch pool.
        /// </summary>
        /// <param name="batchClient">A BatchClient object</param>
        /// <param name="poolId">ID of the CloudPool object to create.</param>
        private static async Task CreatePoolIfNotExistAsync(string poolId)
        {

            var credential = new DefaultAzureCredential();
            ArmClient _armClient = new ArmClient(credential);

            var batchAccountIdentifier = ResourceIdentifier.Parse(BatchAccountResourceID);
            BatchAccountResource batchAccount = await _armClient.GetBatchAccountResource(batchAccountIdentifier).GetAsync();

            BatchAccountPoolCollection collection = batchAccount.GetBatchAccountPools();
            if (collection.Exists(poolId) == false)
            {
                var poolName = poolId;
                var imageReference = new BatchImageReference()
                {
                    Publisher = "MicrosoftWindowsServer",
                    Offer = "WindowsServer",
                    Sku = "2019-datacenter-smalldisk",
                    Version = "latest"
                };
                string nodeAgentSku = "batch.node.windows amd64";


                ArmOperation<BatchAccountPoolResource> armOperation = await batchAccount.GetBatchAccountPools().CreateOrUpdateAsync(
                    WaitUntil.Completed, poolName, new BatchAccountPoolData()
                    {
                        VmSize = "Standard_DS1_v2",
                        DeploymentConfiguration = new BatchDeploymentConfiguration()
                        {
                            VmConfiguration = new BatchVmConfiguration(imageReference, nodeAgentSku)
                        },
                        ScaleSettings = new BatchAccountPoolScaleSettings()
                        {
                            FixedScale = new BatchAccountFixedScaleSettings()
                            {
                                TargetDedicatedNodes = DedicatedNodeCount,
                                TargetLowPriorityNodes = LowPriorityNodeCount
                            }
                        },
                        Identity = new ManagedServiceIdentity(ManagedServiceIdentityType.UserAssigned)
                        {
                            UserAssignedIdentities =
                            {
                                    [new ResourceIdentifier(ManagedIdentityId)] = new Azure.ResourceManager.Models.UserAssignedIdentity(),
                            },
                        },
                        ApplicationPackages =
                        {
                                new Azure.ResourceManager.Batch.Models.BatchApplicationPackageReference(new ResourceIdentifier(appPacakgeResourceID))
                                {
                                    Version = appPackageVersion,
                                }
                        },

                    });
                BatchAccountPoolResource pool = armOperation.Value;
            }

        }


        /// <summary>
        /// Creates a job in the specified pool.
        /// </summary>
        /// <param name="batchClient">A BatchClient object.</param>
        /// <param name="jobId">ID of the job to create.</param>
        /// <param name="poolId">ID of the CloudPool object in which to create the job.</param>
        private static async Task CreateJobAsync(BatchClient batchClient, string jobId, string poolId)
        {
            Console.WriteLine("Creating job [{0}]...", jobId);
            BatchJobCreateContent batchJobCreateContent = new BatchJobCreateContent(jobId, new BatchPoolInfo { PoolId = poolId });
            await batchClient.CreateJobAsync(batchJobCreateContent);
        }
       
        
        /// <summary>
        /// 
        /// </summary>Creates tasks to process each of the specified input files, and submits them
        ///  to the specified job for execution.
        /// <param name="batchClient">A BatchClient object.</param>
        /// <param name="jobId">ID of the job to which the tasks are added.</param>
        /// <param name="inputFiles">A collection of ResourceFile objects representing the input file
        /// to be processed by the tasks executed on the compute nodes.</param>
        /// <param name="outputContainerSasUrl">The shared access signature URL for the Azure 
        /// Storagecontainer that will hold the output files that the tasks create.</param>
        /// <returns>A collection of the submitted cloud tasks.</returns>
        private static async Task AddTasksAsync(BatchClient batchClient, string jobId, List<ResourceFile> inputFiles, string outputContainerSasUrl)
        {
            Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, jobId);

            // Create a collection to hold the tasks added to the job:
            List<BatchTaskCreateContent> tasks = new List<BatchTaskCreateContent>();

            for (int i = 0; i < inputFiles.Count; i++)
            {
                // Assign a task ID for each iteration
                string taskId = String.Format("Task{0}", i);

                // Define task command line to convert the video format from MP4 to MP3 using ffmpeg.
                // Note that ffmpeg syntax specifies the format as the file extension of the input file
                // and the output file respectively. In this case inputs are MP4.
                string appPath = String.Format("%AZ_BATCH_APP_PACKAGE_{0}#{1}%", appPackageId, appPackageVersion);
                string inputMediaFile = inputFiles[i].StorageContainerUrl;
                string outputMediaFile = String.Format("{0}{1}",
                    System.IO.Path.GetFileNameWithoutExtension(inputMediaFile),
                    ".mp3");
                string taskCommandLine = String.Format("cmd /c {0}\\ffmpeg-4.3.1-2020-11-08-full_build\\bin\\ffmpeg.exe -i {1} {2}", appPath, inputMediaFile, outputMediaFile);

                // Create a batch task (with the task ID and command line) and add it to the task list

                BatchTaskCreateContent batchTaskCreateContent = new BatchTaskCreateContent(taskId, taskCommandLine);
                batchTaskCreateContent.ResourceFiles.Add(inputFiles[i]);

                // Task output file will be uploaded to the output container in Storage.
                // TODO: Replace <storage-account-name> with your actual storage account name
                OutputFileBlobContainerDestination outputContainer = new OutputFileBlobContainerDestination("https://<storage-account-name>.blob.core.windows.net/output/" + outputMediaFile)
                {
                    IdentityReference = inputFiles[i].IdentityReference,
                };

                OutputFile outputFile = new OutputFile(outputMediaFile,
                                                       new OutputFileDestination() { Container = outputContainer },
                                                       new OutputFileUploadConfig(OutputFileUploadCondition.TaskSuccess));
                batchTaskCreateContent.OutputFiles.Add(outputFile);

                tasks.Add(batchTaskCreateContent);
            }

            // Call BatchClient.CreateTaskCollectionAsync() to add the tasks as a collection rather than making a
            // separate call for each. Bulk task submission helps to ensure efficient underlying API
            // calls to the Batch service. 

            await batchClient.CreateTaskCollectionAsync(jobId, new BatchTaskGroup(tasks));
        }
        
        /// <summary>
        /// Monitors the specified tasks for completion and whether errors occurred.
        /// </summary>
        /// <param name="batchClient">A BatchClient object.</param>
        /// <param name="jobId">ID of the job containing the tasks to be monitored.</param>
        /// <param name="timeout">The period of time to wait for the tasks to reach the completed state.</param>
        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            bool allTasksSuccessful = true;
            const string completeMessage = "All tasks reached state Completed.";
            const string incompleteMessage = "One or more tasks failed to reach the Completed state within the timeout period.";
            const string successMessage = "Success! All tasks completed successfully. Output files uploaded to output container.";
            const string failureMessage = "One or more tasks failed.";

            // Obtain the collection of tasks currently managed by the job. 
            // Use a detail level to specify that only the "id" property of each task should be populated. 
            // See https://docs.microsoft.com/en-us/azure/batch/batch-efficient-list-queries

            bool completed = false;

            while(completed == false)
            {
                completed = true;
                await foreach (BatchTask item in batchClient.GetTasksAsync(jobId, filter: "state ne 'completed'"))
                {
                    completed = false;

                    Thread.Sleep(TimeSpan.FromSeconds(10));
                    break;
                }
            }
            
            Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout.ToString());

            await batchClient.TerminateJobAsync(jobId);
            Console.WriteLine(completeMessage);

            // All tasks have reached the "Completed" state, however, this does not guarantee all tasks completed successfully.
            // Here we further check for any tasks with an execution result of "Failure".

            // Filter for tasks with 'Failure' result.
            var failedTasks = batchClient.GetTasks(jobId, filter: "executionInfo/result eq 'Failure'", select: ["executionInfo"]);

            if (failedTasks.Any())
            {
                allTasksSuccessful = false;
                Console.WriteLine(failureMessage);
            }
            else
            {
                Console.WriteLine(successMessage);
            }

            return allTasksSuccessful;
        }
        
    }
}
