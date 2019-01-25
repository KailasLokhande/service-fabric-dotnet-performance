Connect-ServiceFabricCluster -ConnectionEndpoint ClusterAddress:19000 -KeepAliveIntervalInSec 10 -X509Credential -ServerCertThumbprint Thumbnail -FindType FindByThumbprint -FindValue Thumbnail -StoreLocation CurrentUser -StoreName My
Remove-ServiceFabricApplication -ApplicationName fabric:/LoadDriverApplication -Force
Remove-ServiceFabricApplication -ApplicationName fabric:/SFDictionaryApplication -Force
Start-Sleep -s 150
cd F:\service-fabric-dotnet-performance\ServiceLoadTest\Framework\LoadDriverApplication\Scripts
.\Deploy-FabricApplication.ps1 -PublishProfileFile ..\PublishProfiles\Cloud.xml -ApplicationPackagePath ..\pkg\Release
cd F:\service-fabric-dotnet-performance\ServiceLoadTest\ServiceFabric\Dictionary\SFDictionaryApplication\Scripts
.\Deploy-FabricApplication.ps1 -PublishProfileFile ..\PublishProfiles\Cloud.xml -ApplicationPackagePath ..\pkg\Release
Start-Sleep -s 300
F:\service-fabric-dotnet-performance\ServiceLoadTest\Framework\TestClient\bin\x64\Release\ServiceLoadTestClient.exe

