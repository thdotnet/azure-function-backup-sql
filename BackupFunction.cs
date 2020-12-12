using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;

namespace AzureFunctionsBackupSQL
{
    public static class BackupFunction
    {
        [FunctionName("BackupFunction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");
            
            var sqlServerName = "<SQL Server Name> (without '.database.windows.net'>";
            var sqlServerResourceGroupName = "<SQL Server Resource Group>";
            
            var databaseName = "<Database Name>";
            var databaseLogin = "<Database Login>";
            var databasePassword = "<Database Password>";

            var storageResourceGroupName = "<Storage Resource Group>";
            var storageName = "<Storage Account>";
            var storageBlobName = "<Storage Blob Name>";

            var bacpacFileName = $"backup{DateTime.UtcNow.ToString("yyyy-MM-dd-hh-mm")}.bacpac";
            
            var credentials = new AzureCredentialsFactory().FromSystemAssignedManagedServiceIdentity(MSIResourceType.AppService, AzureEnvironment.AzureGlobalCloud);
            var azure = await Azure.Authenticate(credentials).WithDefaultSubscriptionAsync();

            var storageAccount = await azure.StorageAccounts.GetByResourceGroupAsync(storageResourceGroupName, storageName);


            var sqlServer = await azure.SqlServers.GetByResourceGroupAsync(sqlServerResourceGroupName, sqlServerName);
            var database = await sqlServer.Databases.GetAsync(databaseName);

            await database.ExportTo(storageAccount, storageBlobName, bacpacFileName)
                    .WithSqlAdministratorLoginAndPassword(databaseLogin, databasePassword)
                    .ExecuteAsync();

            return new OkObjectResult(true);
        }
    }
}
