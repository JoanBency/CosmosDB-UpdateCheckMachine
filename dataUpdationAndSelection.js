const CosmosClient = require("@azure/cosmos").CosmosClient;
const { performance } = require('perf_hooks');
const appSettings = require('./Json_Files/appSettings.json');
const logger = require('./utils/logger').logger;
const DMLQueryStatsLogger = require('./utils/DMLQueryStatsLogger').logger;
const RawDataStatsLogger = require('./utils/RawDataStatsLogger').logger;
const headerLogger = require('./utils/headerLogger').logger;
const minMaxValue = require('./utilities');

var dataUpdation = appSettings.dataUpdation;
var dataSelection = appSettings.dataPointSelection;
var dataIngestion = appSettings.dataIngestion;
var numOfRecords = (dataUpdation[1].productIDEnd - dataUpdation[0].productIDStart + 1)*dataSelection[3].numOfIterations;

var selectMetrics = {
    loopCounter : 0,
    avgRoundtripTime: 0.0,
    minSelectRoundtripTime: 100000.0,
    maxSelectRoundtripTime: 0.0,
    totalRoundtripTime: 0.0,
    avgRequestDuration: 0.0,
    minRequestDuration: 100000.0,
    maxRequestDuration: 0.0,
    totalRequestDuration: 0.0,
    avgRequestCharge: 0.0,
    minRequestUnit: 100000.0,
    maxRequestUnit: 0.0,
    totalRequestUnit: 0.0,
    cacheHitCount: 0,
    pragmaNoCache: 0,
    pragmaElse: 0,
    cacheControlElse: 0,
    numberOfReadRegions: 0,
    avgThrottleRetryCount: 0,
    avgThrottleRetryWaitTime: 0.0,
    totalRecordFetched: 0,
    totalRecordFetchedWithCache: 0,
    totalRecordUpdated: 0,
};

var metaDataLog;

async function UpdationAndSelection(connectionSettings, connectionSettingsDQL, connectionMode, partitionMode, callback) {
    var roundtripStartTime;

    selectMetrics = { avgRoundtripTime: 0.0,minSelectRoundtripTime: 100000.0,maxSelectRoundtripTime: 0.0,totalRoundtripTime: 0.0,avgRequestDuration: 0.0,minRequestDuration: 100000.0,maxRequestDuration: 0.0,totalRequestDuration: 0.0,avgRequestCharge: 0.0,minRequestUnit: 100000.0,maxRequestUnit: 0.0,totalRequestUnit: 0.0,cacheHitCount: 0,pragmaNoCache: 0, pragmaElse: 0, cacheControlElse: 0, numberOfReadRegions: 0, avgThrottleRetryCount: 0, avgThrottleRetryWaitTime: 0.0,totalRecordFetched: 0,totalRecordFetchedWithCache: 0, totalRecordUpdated: 0};

    var logDataJson = appSettings.logger;

    try {
        var failedQueries = 0, debugLogEntry = 0;

        console.log("\nNow we are gonna start updating data...");
        if (partitionMode == "Single Partition") {

            for(i=dataUpdation[0].productIDStart; i<=dataUpdation[1].productIDEnd;i++) {
                const endpoint = connectionSettingsDQL[0].endpoint;
                const key = connectionSettingsDQL[1].key;
                const databaseId = connectionSettingsDQL[2].databaseId;
                const containerId = connectionSettingsDQL[3].containerId;
                const consistencyLevel = connectionSettingsDQL[5].clientOptions.consistencyLevel;
                const preferredLocations = connectionSettingsDQL[6].preferredLocations;
                var client;
                if (appSettings.INCLUDE_CLIENT_OPTIONS) {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations }, consistencyLevel });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key, consistencyLevel });
                    }   
                }
                else {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations } });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key });
                    }  
                }
                const database = client.database(databaseId);
                const container = database.container(containerId);

                var newRecord = getUpdatedRecord(partitionMode, i);
                try {
                    var { resource: updatedRecord } = await container.item(String(i),0).replace(newRecord);
                    selectMetrics.totalRecordUpdated++;
                }catch(err) { 
                    console.log(err.message);
                }
            }
            var roundtripStartTime = performance.now();
            for(i=dataUpdation[0].productIDStart; i<=dataUpdation[1].productIDEnd;i++) {
                const endpoint = connectionSettings[0].endpoint;
                const key = connectionSettings[1].key;
                const databaseId = connectionSettings[2].databaseId;
                const containerId = connectionSettings[3].containerId;
                const consistencyLevel = connectionSettings[5].clientOptions.consistencyLevel;
                const preferredLocations = connectionSettings[6].preferredLocations;
                var client;
                if (appSettings.INCLUDE_CLIENT_OPTIONS) {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations }, consistencyLevel });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key, consistencyLevel });
                    }   
                }
                else {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations } });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key });
                    }  
                }
                const database = client.database(databaseId);
                const container = database.container(containerId);

                for(j = 0; j < dataSelection[3].numOfIterations; j++) {
                    var selectRoundtripStartTime = performance.now();
    
                    try {
                        const { resource: selectedRecord, headers: headerInformation } = await container.item(String(i),0).read();
                            
                        RawDataStatsLogger.info(JSON.stringify(selectedRecord));
                        RawDataStatsLogger.info("**********************************************");

                        if (debugLogEntry < logDataJson[0].debugLogRecords) {
                            headerLogger.info(JSON.stringify(headerInformation));
                            headerLogger.info("**********************************************");
                            debugLogEntry++;
                        }
                        var selectRoundtripEndTime = performance.now();
                        selectRoundtripTime = selectRoundtripEndTime - selectRoundtripStartTime;
                        calculateQueryMetrics(selectRoundtripTime, headerInformation, selectedRecord);
                    }catch (err) {
                        failedQueries++;
                        console.log(err.message);
                    }
                    RawDataStatsLogger.info("==============================================");
                }
            }
        }
        else if (partitionMode == "Multiple Partition") {
            
            for(i=dataUpdation[0].productIDStart; i<=dataUpdation[1].productIDEnd;i++) {
                const endpoint = connectionSettingsDQL[0].endpoint;
                const key = connectionSettingsDQL[1].key;
                const databaseId = connectionSettingsDQL[2].databaseId;
                const containerId = connectionSettingsDQL[3].containerId;
                const consistencyLevel = connectionSettingsDQL[5].clientOptions.consistencyLevel;
                const preferredLocations = connectionSettingsDQL[6].preferredLocations;
                var client;
                if (appSettings.INCLUDE_CLIENT_OPTIONS) {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations }, consistencyLevel });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key, consistencyLevel });
                    }   
                }
                else {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations } });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key });
                    }  
                }
                const database = client.database(databaseId);
                const container = database.container(containerId);

                var newRecord = getUpdatedRecord(partitionMode, i);
                try {
                    var { resource: updatedRecord } = await container.item(String(i),(i%10)).replace(newRecord);
                    selectMetrics.totalRecordUpdated++;
                }catch(err) { 
                    console.log(err.message); 
                }
            }
            var roundtripStartTime = performance.now();
            for(i=dataUpdation[0].productIDStart; i<=dataUpdation[1].productIDEnd;i++) {
                const endpoint = connectionSettings[0].endpoint;
                const key = connectionSettings[1].key;
                const databaseId = connectionSettings[2].databaseId;
                const containerId = connectionSettings[3].containerId;
                const consistencyLevel = connectionSettings[5].clientOptions.consistencyLevel;
                const preferredLocations = connectionSettings[6].preferredLocations;
                var client;
                if (appSettings.INCLUDE_CLIENT_OPTIONS) {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations }, consistencyLevel });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key, consistencyLevel });
                    }   
                }
                else {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations } });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key });
                    }  
                }
                const database = client.database(databaseId);
                const container = database.container(containerId);

                for(j = 0; j < dataSelection[3].numOfIterations; j++) {
                    var selectRoundtripStartTime = performance.now();
    
                    try {
                        const { resource: selectedRecord, headers: headerInformation } = await container.item(String(i),(i%10)).read();
    
                        RawDataStatsLogger.info(JSON.stringify(selectedRecord));
                        RawDataStatsLogger.info("**********************************************");

                        if (debugLogEntry < logDataJson[0].debugLogRecords) {
                            headerLogger.info(JSON.stringify(headerInformation));
                            headerLogger.info("**********************************************");
                            debugLogEntry++;
                        }
                        var selectRoundtripEndTime = performance.now();
                        selectRoundtripTime = selectRoundtripEndTime - selectRoundtripStartTime;
                        calculateQueryMetrics(selectRoundtripTime, headerInformation, selectedRecord);
                    }catch (err) {
                        failedQueries++;
                        console.log(err.message);
                    }
                }
                RawDataStatsLogger.info("=============================================="); 
            }
        }
        logSelectProcessDesc(connectionSettings, connectionMode, failedQueries, partitionMode);

    }catch(err) {
        console.log(err.message);
    }
    var roundtripEndTime = performance.now();

    selectMetrics.totalRoundtripTime = (roundtripEndTime - roundtripStartTime).toFixed(2);
    selectMetrics.avgRoundtripTime = (selectMetrics.totalRoundtripTime/numOfRecords).toFixed(3);

    selectMetrics.avgRequestDuration = (selectMetrics.totalRequestDuration/numOfRecords).toFixed(3);

    selectMetrics.avgRequestCharge = (selectMetrics.totalRequestUnit/numOfRecords).toFixed(3);

    selectMetrics.avgThrottleRetryCount = (selectMetrics.avgThrottleRetryCount/numOfRecords).toFixed(3);

    selectMetrics.avgThrottleRetryWaitTime = (selectMetrics.avgThrottleRetryWaitTime/numOfRecords).toFixed(3);

    logSelectResponseStats();
    callback();
}


async function Selection(connectionSettings, connectionMode, partitionMode, callback) {
    var roundtripStartTime = performance.now();

    selectMetrics = { avgRoundtripTime: 0.0,minSelectRoundtripTime: 100000.0,maxSelectRoundtripTime: 0.0,totalRoundtripTime: 0.0,avgRequestDuration: 0.0,minRequestDuration: 100000.0,maxRequestDuration: 0.0,totalRequestDuration: 0.0,avgRequestCharge: 0.0,minRequestUnit: 100000.0,maxRequestUnit: 0.0,totalRequestUnit: 0.0,cacheHitCount: 0,pragmaNoCache: 0, pragmaElse: 0, cacheControlElse: 0, numberOfReadRegions: 0, avgThrottleRetryCount: 0, avgThrottleRetryWaitTime: 0.0,totalRecordFetched: 0,totalRecordFetchedWithCache: 0, totalRecordUpdated: 0};

    const endpoint = connectionSettings[0].endpoint;
    const key = connectionSettings[1].key;
    const databaseId = connectionSettings[2].databaseId;
    const containerId = connectionSettings[3].containerId;

    const consistencyLevel = connectionSettings[5].clientOptions.consistencyLevel;
    const preferredLocations = connectionSettings[6].preferredLocations;

    var logDataJson = appSettings.logger;

    try {
        var failedQueries = 0, debugLogEntry = 0;
        // console.log("\nNow we are gonna start updating data...");
        if (partitionMode == "Single Partition") {
            for(i=dataUpdation[0].productIDStart; i<=dataUpdation[1].productIDEnd;i++) {
                selectMetrics.totalRecordUpdated++;

                var client;
                if (appSettings.INCLUDE_CLIENT_OPTIONS) {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations }, consistencyLevel });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key, consistencyLevel });
                    }   
                }
                else {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations } });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key });
                    }  
                }

                const database = client.database(databaseId);
                const container = database.container(containerId);

                for(j = 0; j < dataSelection[3].numOfIterations; j++) {
                    var selectRoundtripStartTime = performance.now();
    
                    try {
                        const { resource: selectedRecord, headers: headerInformation } = await container.item(String(i),0).read();
                            
                        RawDataStatsLogger.info(JSON.stringify(selectedRecord));
                        RawDataStatsLogger.info("**********************************************");

                        if (debugLogEntry < logDataJson[0].debugLogRecords) {
                            headerLogger.info(JSON.stringify(headerInformation));
                            headerLogger.info("**********************************************");
                            debugLogEntry++;
                        }
                        var selectRoundtripEndTime = performance.now();
                        selectRoundtripTime = selectRoundtripEndTime - selectRoundtripStartTime;
                        calculateQueryMetrics2(selectRoundtripTime, headerInformation, selectedRecord);
                    }catch (err) {
                        failedQueries++;
                        console.log(err.message);
                    }
                    RawDataStatsLogger.info("==============================================");
                }
            }
        }
        else if (partitionMode == "Multiple Partition") {
            for(i=dataUpdation[0].productIDStart; i<=dataUpdation[1].productIDEnd;i++) {
                selectMetrics.totalRecordUpdated++;

                var client;
                if (appSettings.INCLUDE_CLIENT_OPTIONS) {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations }, consistencyLevel });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key, consistencyLevel });
                    }   
                }
                else {
                    if (appSettings.INCLUDE_MULTIPLE_REGIONS) {
                        client = new CosmosClient({ endpoint, key, connectionPolicy: { preferredLocations } });
                    }
                    else {
                        client = new CosmosClient({ endpoint, key });
                    }  
                }

                const database = client.database(databaseId);
                const container = database.container(containerId);

                for(j = 0; j < dataSelection[3].numOfIterations; j++) {
                    var selectRoundtripStartTime = performance.now();
    
                    try {
                        const { resource: selectedRecord, headers: headerInformation } = await container.item(String(i),(i%10)).read();
    
                        RawDataStatsLogger.info(JSON.stringify(selectedRecord));
                        RawDataStatsLogger.info("**********************************************");

                        if (debugLogEntry < logDataJson[0].debugLogRecords) {
                            headerLogger.info(JSON.stringify(headerInformation));
                            headerLogger.info("**********************************************");
                            debugLogEntry++;
                        }
                        var selectRoundtripEndTime = performance.now();
                        selectRoundtripTime = selectRoundtripEndTime - selectRoundtripStartTime;
                        calculateQueryMetrics2(selectRoundtripTime, headerInformation, selectedRecord);
                    }catch (err) {
                        failedQueries++;
                        console.log(err.message);
                    }
    
                }
                RawDataStatsLogger.info("==============================================");
            }
        }
        logSelectProcessDesc(connectionSettings, connectionMode, failedQueries, partitionMode);

    }catch(err) {
        console.log(err.message);
    }
    var roundtripEndTime = performance.now();

    selectMetrics.totalRoundtripTime = (roundtripEndTime - roundtripStartTime).toFixed(2);
    selectMetrics.avgRoundtripTime = (selectMetrics.totalRoundtripTime/numOfRecords).toFixed(3);

    selectMetrics.avgRequestDuration = (selectMetrics.totalRequestDuration/numOfRecords).toFixed(3);

    selectMetrics.avgRequestCharge = (selectMetrics.totalRequestUnit/numOfRecords).toFixed(3);

    selectMetrics.avgThrottleRetryCount = (selectMetrics.avgThrottleRetryCount/numOfRecords).toFixed(3);

    selectMetrics.avgThrottleRetryWaitTime = (selectMetrics.avgThrottleRetryWaitTime/numOfRecords).toFixed(3);

    logSelectResponseStats();
    callback();
}



function getUpdatedRecord(partitionMode, i) {

    var partitionID;
    if ( partitionMode == "Single Partition" ) { 
        partitionID = 0;  
    }
    else if ( partitionMode == "Multiple Partition" ) { 
        partitionID = i % 10;  
    }
    else { partitionID = null; }

    var newProduct = {
        id: String(i),
        ProductId: i,
        ProductName: `Electronics${i}`,
        CategoryName: dataIngestion[2].categoryList[Math.floor(Math.random()*dataIngestion[2].categoryList.length)],
        SubCategory: dataIngestion[3].subCategoryList[Math.floor(Math.random()*dataIngestion[3].subCategoryList.length)],
        Description: 'My Product Description - V2',
        Manufacturer: 'IBM',
        Price: Math.round(Math.random() * (dataIngestion[4].priceRange[1] - dataIngestion[4].priceRange[0]) + dataIngestion[4].priceRange[0]),
        PartitionID: partitionID
    };

    return newProduct;
}


function calculateQueryMetrics(selectRoundtripTime, headerInformation, selectedRecord) {
    var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(selectRoundtripTime, selectMetrics.minSelectRoundtripTime, selectMetrics.maxSelectRoundtripTime);
        selectMetrics.minSelectRoundtripTime = newMinValue;
        selectMetrics.maxSelectRoundtripTime = newMaxValue;

        //Request Duration 
        var requestDuration = parseFloat(headerInformation['x-ms-request-duration-ms']);
        var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(requestDuration, selectMetrics.minRequestDuration, selectMetrics.maxRequestDuration);
        selectMetrics.minRequestDuration = newMinValue;
        selectMetrics.maxRequestDuration = newMaxValue;
        selectMetrics.totalRequestDuration = selectMetrics.totalRequestDuration + requestDuration;
                
        // Request Charge
        var requestCharge = parseFloat(headerInformation['x-ms-request-charge']);

        var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(requestCharge, selectMetrics.minRequestUnit, selectMetrics.maxRequestUnit);
        selectMetrics.minRequestUnit = newMinValue;
        selectMetrics.maxRequestUnit = newMaxValue;
        selectMetrics.totalRequestUnit = selectMetrics.totalRequestUnit + requestCharge;

        // Query Metrics

        if (selectedRecord.Description == "My Product Description - V2") {
            selectMetrics.totalRecordFetched++;
        }
        

        // Cache Hit
        var cacheHit = (headerInformation['x-ms-cosmos-cachehit']);

        if(cacheHit) {
		if (cacheHit.toUpperCase() == "TRUE") {
            	selectMetrics.cacheHitCount++;
                if (selectedRecord.Description == "My Product Description - V2") {
                    selectMetrics.totalRecordFetchedWithCache++;
                }
	    	}
        }

        if (headerInformation.pragma == 'no-cache') { selectMetrics.pragmaNoCache++; }
        else { selectMetrics.pragmaElse++; }

        if (headerInformation['cache-control'] != 'no-store, no-cache') { selectMetrics.cacheControlElse++;  }

        selectMetrics.numberOfReadRegions += parseInt(headerInformation['x-ms-number-of-read-regions']);

        selectMetrics.avgThrottleRetryCount += headerInformation['x-ms-throttle-retry-count'];

        selectMetrics.avgThrottleRetryWaitTime += headerInformation['x-ms-throttle-retry-wait-time-ms'];
}

function calculateQueryMetrics2(selectRoundtripTime, headerInformation, selectedRecord) {
    var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(selectRoundtripTime, selectMetrics.minSelectRoundtripTime, selectMetrics.maxSelectRoundtripTime);
        selectMetrics.minSelectRoundtripTime = newMinValue;
        selectMetrics.maxSelectRoundtripTime = newMaxValue;

        //Request Duration 
        var requestDuration = parseFloat(headerInformation['x-ms-request-duration-ms']);
        var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(requestDuration, selectMetrics.minRequestDuration, selectMetrics.maxRequestDuration);
        selectMetrics.minRequestDuration = newMinValue;
        selectMetrics.maxRequestDuration = newMaxValue;
        selectMetrics.totalRequestDuration = selectMetrics.totalRequestDuration + requestDuration;
                
        // Request Charge
        var requestCharge = parseFloat(headerInformation['x-ms-request-charge']);

        var { newMinValue, newMaxValue } = minMaxValue.calculateMinMax(requestCharge, selectMetrics.minRequestUnit, selectMetrics.maxRequestUnit);
        selectMetrics.minRequestUnit = newMinValue;
        selectMetrics.maxRequestUnit = newMaxValue;
        selectMetrics.totalRequestUnit = selectMetrics.totalRequestUnit + requestCharge;

        // Query Metrics

        if (selectedRecord.Description == "My Product Description - V1") {
            selectMetrics.totalRecordFetched++;
        }
        

        // Cache Hit
        var cacheHit = (headerInformation['x-ms-cosmos-cachehit']);

        if(cacheHit) {
		if (cacheHit.toUpperCase() == "TRUE") {
            	selectMetrics.cacheHitCount++;
                if (selectedRecord.Description == "My Product Description - V1") {
                    selectMetrics.totalRecordFetchedWithCache++;
                }
	    	}
        }

        if (headerInformation.pragma == 'no-cache') { selectMetrics.pragmaNoCache++; }
        else { selectMetrics.pragmaElse++; }

        if (headerInformation['cache-control'] != 'no-store, no-cache') { selectMetrics.cacheControlElse++;  }

        selectMetrics.numberOfReadRegions += parseInt(headerInformation['x-ms-number-of-read-regions']);

        selectMetrics.avgThrottleRetryCount += headerInformation['x-ms-throttle-retry-count'];

        selectMetrics.avgThrottleRetryWaitTime += headerInformation['x-ms-throttle-retry-wait-time-ms'];
}


function logSelectProcessDesc(connectionSettings, connectionMode, failedQueries, partitionMode) {

    if (appSettings.INCLUDE_CLIENT_OPTIONS) {
        var consistencyLevel = connectionSettings[5].clientOptions.consistencyLevel;
    }
    else {
        var consistencyLevel = "NOT SET"
    }
    var partitionKeyGiven = "Yes";
    var scenarioType = `UPD_PSEL2-DGW_${connectionMode == "DedicatedGateway" ? "Yes" : connectionMode == "DirectConnection" ? "No" : null}-${partitionMode == "Single Partition" ? "SP" : partitionMode == "Multiple Partition" ? "MP" : null}-PK_${partitionKeyGiven}-Run_Num#${dataSelection[2].Run_Num}`;
    console.log("\n==============================================");
    console.log("\nProcess Desc: \tUpdation and Point Selecting(V2) Data Records");
    console.log(`\nConsistency level: \t${consistencyLevel}`);
    console.log(`\nScenario Type: \t${scenarioType}`);
    console.log(`\nDedicated GW Used: \t${connectionMode == "DedicatedGateway" ? "Yes" : connectionMode == "DirectConnection" ? "No" : null}`);
    console.log(`\nCosmos DB Endpoint: \t${connectionSettings[0].endpoint}`);
    console.log(`\nDB Name: \t${connectionSettings[2].databaseId}`);
    console.log(`\nCollection Name: \t${connectionSettings[3].containerId}`);
    console.log(`\nPartition Scenario: \t${partitionMode}, ${partitionKeyGiven == "Yes" ? "Given" : partitionKeyGiven == "No" ? "Not given" : null }`);
    console.log(`\nPartition Key Name: \t${connectionSettings[4].partitionKey.paths[0]}`);
    console.log(`\nSuccessful queries: \t${numOfRecords-failedQueries}`);
    console.log(`\nQueries Failed: \t${failedQueries}`);
    console.log(`\nTotal Queries Processed: \t${numOfRecords}`);

    // _______________________________________________________________________________
    // Logging into file
    logger.info("====================PROCESS START==========================");
    logger.info("Process Desc: \tUpdation and Point Selecting(V2) Data Records");
    logger.info(`Consistency level: \t${consistencyLevel}`);
    logger.info(`Scenario Type: \t${scenarioType}`);
    logger.info(`Dedicated GW Used: \t${connectionMode == "DedicatedGateway" ? "Yes" : connectionMode == "DirectConnection" ? "No" : null}`);
    logger.info(`Cosmos DB Endpoint: \t${connectionSettings[0].endpoint}`);
    logger.info(`DB Name: \t${connectionSettings[2].databaseId}`);
    logger.info(`Collection Name: \t${connectionSettings[3].containerId}`);
    logger.info(`Partition Scenario: \t${partitionMode}, ${partitionKeyGiven == "Yes" ? "Given" : partitionKeyGiven == "No" ? "Not given" : null }`);
    logger.info(`Partition Key Name: \t${connectionSettings[4].partitionKey.paths[0]}`);
    logger.info(`Successful queries: \t${numOfRecords-failedQueries}`);
    logger.info(`Queries Failed: \t${failedQueries}`);
    logger.info(`Total Queries Processed: \t${numOfRecords}`);

    var dedicatedGatewayUsed = connectionMode == "DedicatedGateway" ? 'Yes' : connectionMode == "DirectConnection" ? 'No' : 'null';
    var partitionKeyGivenOrNot =  partitionKeyGiven == "Yes" ? 'Given' : partitionKeyGiven == "No" ? 'Not given' : null;
    // DMLQueryStats.csv file log
   metaDataLog = 'Updation and Point Selecting(V2) Data Records,' + consistencyLevel + ',' + scenarioType + ',' + dedicatedGatewayUsed + ',' + connectionSettings[0].endpoint + ',' + connectionSettings[2].databaseId + ','+ connectionSettings[3].containerId + ',' + partitionMode + '-' + partitionKeyGivenOrNot + ',' + connectionSettings[4].partitionKey.paths[0] + ',' + String(numOfRecords-failedQueries) + ',' + String(failedQueries) + ',' + String(numOfRecords) + ',';
}


function logSelectResponseStats() {

    console.log(`\nQuery Response Stats\n---------------------`);
    console.log(`\nTotal Records updated: \t${selectMetrics.totalRecordUpdated}`);
    console.log(`\nTotal records found updated immediately: \t${selectMetrics.totalRecordFetched}`);
    console.log(`\nTotal Records fetched with cache: \t${selectMetrics.totalRecordFetchedWithCache}`);
    console.log(`\nTotal Cache Hit('x-ms-cosmos-cachehit'): \t${selectMetrics.cacheHitCount}`);
    console.log(`\nNumber of times pragma is no-cache: \t${selectMetrics.pragmaNoCache}`);
    console.log(`\nNumber of times pragma is not no-cache: \t${selectMetrics.pragmaElse}`);
    console.log(`\nNumber of times cache-control is not no-store and no-cache: \t${selectMetrics.cacheControlElse}`);
    console.log(`\nTotal Number of read regions: \t${selectMetrics.numberOfReadRegions}`);
    console.log(`\nAverage of Throttle Retry Count: \t${selectMetrics.avgThrottleRetryCount}`);
    console.log(`\nAverage of Throttle Retry Wait Time: \t${selectMetrics.avgThrottleRetryWaitTime}`);
    console.log("\n**********************************************");
    console.log(`\nProcess Round Trip : Avg Time to process a single Records: \t${selectMetrics.avgRoundtripTime} Millis`);
    console.log(`\nProcess Round Trip : Min Time taken to select: \t${selectMetrics.minSelectRoundtripTime.toFixed(3)} Millis`);
    console.log(`\nProcess Round Trip : Max Time taken to select: \t${selectMetrics.maxSelectRoundtripTime.toFixed(3)} Millis`);
    console.log(`\nProcess Round Trip : Total Time taken to select: \t${selectMetrics.totalRoundtripTime} Millis`);
    console.log("\n**********************************************");
    console.log(`\nCosmos DB: Avg Time to process a single Records: \t${selectMetrics.avgRequestDuration} Millis`);
    console.log(`\nCosmos DB: Min Time taken to select: \t${selectMetrics.minRequestDuration} Millis`);
    console.log(`\nCosmos DB: Max Time taken to select: \t${selectMetrics.maxRequestDuration} Millis`);
    console.log(`\nCosmos DB: Total Time taken to select: \t${selectMetrics.totalRequestDuration.toFixed(3)} Millis`);
    console.log("\n**********************************************");
    console.log(`\nDifference in Avg Time to Execute Query ( NW + Compute Delay): \t${(selectMetrics.avgRoundtripTime - selectMetrics.avgRequestDuration).toFixed(3)} Millis`);
    console.log(`\nDifference in Total Time to Execute Query ( NW + Compute Delay): \t${(selectMetrics.totalRoundtripTime - selectMetrics.totalRequestDuration).toFixed(3)} Millis`);
    console.log("\n**********************************************");
    console.log(`\nAvg RU to process a single Records: \t${selectMetrics.avgRequestCharge}`);
    console.log(`\nMin RU taken to select: \t${selectMetrics.minRequestUnit}`);
    console.log(`\nMax RU taken to select: \t${selectMetrics.maxRequestUnit}`);
    console.log(`\nTotal RU taken to select: \t${selectMetrics.totalRequestUnit.toFixed(3)}`);
    console.log("\n==============================================");

    // _______________________________________________________________________________
    // Logging into file
    logger.info(`Query Response Stats`);
    logger.info(`---------------------`);
    logger.info(`Total Records updated: \t${selectMetrics.totalRecordUpdated}`);
    logger.info(`Total records found updated immediately: \t${selectMetrics.totalRecordFetched}`);
    logger.info(`Total Records fetched with cache: \t${selectMetrics.totalRecordFetchedWithCache}`);
    logger.info(`Total Cache Hit('x-ms-cosmos-cachehit'): \t${selectMetrics.cacheHitCount}`);
    logger.info(`Number of times pragma is no-cache: \t${selectMetrics.pragmaNoCache}`);
    logger.info(`Number of times pragma is not no-cache: \t${selectMetrics.pragmaElse}`);
    logger.info(`Number of times cache-control is not no-store and no-cache: \t${selectMetrics.cacheControlElse}`);
    logger.info(`Total Number of read regions: \t${selectMetrics.numberOfReadRegions}`);
    logger.info(`Average of Throttle Retry Count: \t${selectMetrics.avgThrottleRetryCount}`);
    logger.info(`Average of Throttle Retry Wait Time: \t${selectMetrics.avgThrottleRetryWaitTime}`);
    logger.info("**********************************************");
    logger.info(`Process Round Trip : Avg Time to process a single Records: \t${selectMetrics.avgRoundtripTime} Millis`);
    logger.info(`Process Round Trip : Min Time taken to select: \t${selectMetrics.minSelectRoundtripTime.toFixed(3)} Millis`);
    logger.info(`Process Round Trip : Max Time taken to select: \t${selectMetrics.maxSelectRoundtripTime.toFixed(3)} Millis`);
    logger.info(`Process Round Trip : Total Time taken to select: \t${selectMetrics.totalRoundtripTime} Millis`);
    logger.info("**********************************************");
    logger.info(`Cosmos DB: Avg Time to process a single Records: \t${selectMetrics.avgRequestDuration} Millis`);
    logger.info(`Cosmos DB: Min Time taken to select: \t${selectMetrics.minRequestDuration} Millis`);
    logger.info(`Cosmos DB: Max Time taken to select: \t${selectMetrics.maxRequestDuration} Millis`);
    logger.info(`Cosmos DB: Total Time taken to select: \t${selectMetrics.totalRequestDuration.toFixed(3)} Millis`);
    logger.info("**********************************************");
    logger.info(`Difference in Avg Time to Execute Query ( NW + Compute Delay): \t${(selectMetrics.avgRoundtripTime - selectMetrics.avgRequestDuration).toFixed(3)} Millis`);
    logger.info(`Difference in Total Time to Execute Query ( NW + Compute Delay): \t${(selectMetrics.totalRoundtripTime - selectMetrics.totalRequestDuration).toFixed(3)} Millis`);
    logger.info("**********************************************");
    logger.info(`Avg RU to process a single Records: \t${selectMetrics.avgRequestCharge}`);
    logger.info(`Min RU taken to select: \t${selectMetrics.minRequestUnit}`);
    logger.info(`Max RU taken to select: \t${selectMetrics.maxRequestUnit}`);
    logger.info(`Total RU taken to select: \t${selectMetrics.totalRequestUnit.toFixed(3)}`);
    logger.info("========================PROCESS END======================");


    // DMLQueryStats.csv file log
    // DMLQueryStatsLogger.info('YYYY-MM-DD HH:MM:SS:MS,PROCESS_DESC,CONSISTENCY_LEVEL,SCENARIO_TYPE,DGW USED?,DB_ENDPOINT,DB_NAME,COLL_NAME,PART_SCENARIO,PART_KEY_NAME,NUM_RECORDS,FAILED_RECORDS,PROCESSED_RECORDS,SAMPLE_QUERY,TOT_REC_UPDATED,TOT_REC_FETCH_CACHE,TOT_CACHE_HIT,PRAGMA=NO-CACHE,PRAGMA!=NO-CACHE,CACHE-CONTROL!="no-store;no-cache",NUM_READ_REGIONS,AVG_THROTTLE_RTRY_COUNT,AVG_RTRY_WAIT_TIME,PRT:PROCESS_AVG_TIME,PRT:PROCESS_MIN_TIME,PRT:PROCESS_MAX_TIME,PRT:PROCESS_TOTAL_TIME,CoDB:PROCESS_AVG_TIME,CoDB:PROCESS_MIN_TIME,CoDB:PROCESS_MAX_TIME,CoDB:PROCESS_TOTAL_TIME,DIFF_AVG_TIME,DIFF_TOTAL_TIME,PROCESS_AVG_RU,PROCESS_MIN_RU,PROCESS_MAX_RU,PROCESS_TOTAL_RU');
    DMLQueryStatsLogger.info(`${metaDataLog},${selectMetrics.totalRecordUpdated},${selectMetrics.totalRecordFetched},${selectMetrics.totalRecordFetchedWithCache},${selectMetrics.cacheHitCount},${selectMetrics.pragmaNoCache},${selectMetrics.pragmaElse},${selectMetrics.cacheControlElse},${selectMetrics.numberOfReadRegions},${selectMetrics.avgThrottleRetryCount},${selectMetrics.avgThrottleRetryWaitTime},${selectMetrics.avgRoundtripTime},${selectMetrics.minSelectRoundtripTime.toFixed(3)},${selectMetrics.maxSelectRoundtripTime.toFixed(3)},${selectMetrics.totalRoundtripTime},${selectMetrics.avgRequestDuration},${selectMetrics.minRequestDuration},${selectMetrics.maxRequestDuration},${selectMetrics.totalRequestDuration.toFixed(3)},${(selectMetrics.avgRoundtripTime - selectMetrics.avgRequestDuration).toFixed(3)},${(selectMetrics.totalRoundtripTime - selectMetrics.totalRequestDuration).toFixed(3)},${selectMetrics.avgRequestCharge},${selectMetrics.minRequestUnit},${selectMetrics.maxRequestUnit},${selectMetrics.totalRequestUnit.toFixed(3)}`);
}

module.exports.dataUpdationAndSelection = { UpdationAndSelection, Selection }