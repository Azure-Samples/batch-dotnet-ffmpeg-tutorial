---
services: batch, storage
platforms: dotnet
author: dlepow
---

# Azure Batch .NET File Processing with ffmpeg

A .NET application that uses Batch to process media files in parallel with the [ffmpeg](http://ffmpeg.org/) open-source tool. 


## Prerequisites

- Azure Batch account and linked general-purpose Azure Storage account
- Visual Studio 2015 or later
- Windows 64-bit version of [ffmpeg 3.4](https://ffmpeg.zeranoe.com/builds/win64/static/ffmpeg-3.4-win64-static.zip)
- Add ffmpeg as an [application package](https://docs.microsoft.com/azure/batch/batch-application-packages) to your Batch account (Application Id: *ffmpeg*, Version: *3.4*)

## Resources

- [Azure Batch documentation](https://docs.microsoft.com/azure/batch/)
- [Azure Batch code samples repo](https://github.com/Azure/azure-batch-samples)

## Project code of conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
