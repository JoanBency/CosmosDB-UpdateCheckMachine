// const prompt = require('prompt');
const cosmosDBConnectionSettings = require("./Json_Files/cosmosDBConnectionSettings.json");
const cosmosDBConnectionSettingsDQL = require('./Json_Files/cosmosDBConnectionSettingsDQL.json');
const dataUpdationAndSelection = require('./dataUpdationAndSelection').dataUpdationAndSelection;

async function main() {
    var connectionSettings, connectionMode, partitionMode;
    connectionSettings = cosmosDBConnectionSettings.cosmosDBDedicatedGatewayNoPartition;
    connectionMode = "DedicatedGateway";
    partitionMode = "Single Partition";
    dataUpdationAndSelection.Selection(connectionSettings, connectionMode, partitionMode, function() {
        var connectionSettings, connectionMode, partitionMode;
        connectionSettings = cosmosDBConnectionSettings.cosmosDBDedicatedGatewayWithPartition;
        connectionMode = "DedicatedGateway";
        partitionMode = "Multiple Partition";
        dataUpdationAndSelection.Selection(connectionSettings, connectionMode, partitionMode, function() {
            var connectionSettings, connectionSettingsDQL, connectionMode, partitionMode;
            connectionSettings = cosmosDBConnectionSettings.cosmosDBDedicatedGatewayNoPartition;
            connectionSettingsDQL = cosmosDBConnectionSettingsDQL.cosmosDBDedicatedGatewayNoPartition;
            connectionMode = "DedicatedGateway";
            partitionMode = "Single Partition";
            dataUpdationAndSelection.UpdationAndSelection(connectionSettings, connectionSettingsDQL, connectionMode, partitionMode, function() {
                var connectionSettings, connectionSettingsDQL, connectionMode, partitionMode;
                connectionSettings = cosmosDBConnectionSettings.cosmosDBDedicatedGatewayWithPartition;
                connectionSettingsDQL = cosmosDBConnectionSettingsDQL.cosmosDBDedicatedGatewayWithPartition;
                connectionMode = "DedicatedGateway";
                partitionMode = "Multiple Partition";
                dataUpdationAndSelection.UpdationAndSelection(connectionSettings, connectionSettingsDQL, connectionMode, partitionMode, function() {
                });
            });
        });
    });
    
}


main();