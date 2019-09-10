---
page_type: sample
languages:
- csharp
products:
- azure
description: "A .NET application that uses Batch to process media files in parallel with the ffmpeg open-source tool."
urlFragment: batch-dotnet-ffmpeg-tutorial
---

# Azure Batch .NET File Processing with ffmpeg

A .NET application that uses Batch to process media files in parallel with the [ffmpeg](http://ffmpeg.org/) open-source tool. 

For details and explanation, see the accompanying article [Run a parallel workload with Azure Batch using the .NET API](https://docs.microsoft.com/azure/batch/tutorial-parallel-dotnet).


## Prerequisites

- Azure Batch account and linked general-purpose Azure Storage account
- Visual Studio 2017, or [.NET Core 2.1](https://www.microsoft.com/net/download/dotnet-core/2.1) for Linux, macOS, or Windows
- Windows 64-bit version of [ffmpeg 3.4](https://ffmpeg.zeranoe.com/builds/win64/static/ffmpeg-3.4-win64-static.zip)
- Add ffmpeg as an [application package](https://docs.microsoft.com/azure/batch/batch-application-packages) to your Batch account (Application Id: *ffmpeg*, Version: *3.4*)

## Resources

- [Azure Batch documentation](https://docs.microsoft.com/azure/batch/)
- [Azure Batch code samples repo](https://github.com/Azure-Samples/azure-batch-samples)

## Project code of conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
