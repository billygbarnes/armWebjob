/**
 * ABB LightHouse
 * Copyright 2017
 * Author: Rahul Jaiswal
 * Created :  Sep. 29, 2017
 * Last Revised By: Rahul Jaiswal
 * Last Updated : Nov 30, 2017
 */

/**
 * wjdatamap Class is used to read device data messages from service bus, process it furthur to make appropriate calls to gateway adaptor apis
 */

var azure = require('azure-sb');
var request = require('request');
//var log4js = require('log4js');

var gatewayMessageAPI = "Property";
var DeviceStatusAPI = "DeviceStatus";
var historianMessageAPI = "Historian";
var alarmLogAPI = "Alarm";
var isLastMessageProcessed = false;
var isDeviceOffline = false;

/*var logFileName = "logs/payload_" + new Date().getTime() + ".log";

log4js.configure({
    appenders: {
        payload: {
            type: 'file',
            filename: "logs/payload.log", 
            maxLogSize: 52428800, 
            backups: 9999999, 
            compress: true
        }
    },
    categories: {
        default: {
            appenders: ['payload'],
            level: 'ALL'
        }
    }
});

var logger = log4js.getLogger('payload');*/

/**
 * This function retuns current date and time in YYYY-MM-DD HH:MM:SS:MM
 */
function getCurrentDateTime() {
    var now = new Date();
    var year = "" + now.getFullYear();
    var month = "" + (now.getMonth() + 1);
    if (month.length == 1) {
        month = "0" + month;
    }

    var day = "" + now.getDate();
    if (day.length == 1) {
        day = "0" + day;
    }

    var hour = "" + now.getHours();
    if (hour.length == 1) {
        hour = "0" + hour;
    }

    var minute = "" + now.getMinutes();
    if (minute.length == 1) {
        minute = "0" + minute;
    }

    var second = "" + now.getSeconds();
    if (second.length == 1) {
        second = "0" + second;
    }

    var millisecond = "" + now.getMilliseconds();
    if (millisecond.length == 1) {
        millisecond = "0" + millisecond;
    }
    return year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second + ":" + millisecond;
}



/**
 * This function makes call to GatewayMessage API with deviceName, appName, tagName and tagValue
 * @param {string} : jsonPayLoad -- This payload contains deviceName, appName, tagName and tagValue
 * @param {string} : methodName -- API name which needs to be called
 * @param {function} : callback -- It's a callback function which will be called once we receive the response of the http request call
 */
function processGatewayMessage(jsonPayLoad, methodName, requestMethod, callback) {

    console.log(jsonPayLoad);


};

/**
 * This function Loop the key/vals and ignore the non-data items. Map gwName to <RMC ID>, devName to <APP ID>
 * @param {string} : body -- This payload contains deviceName, appName, tagName and tagValue *
 * body is Redigate JSON: {"d":{"gwName":"RG400ABB03","devName":"WM_02_PLUNGER","AP":126.3394,"CASING":222.5090,"TUBING":206.1631,"LINE":111.6094,
 * "StateTimer":97,"PlungerArrivingTimer":97,"devIsAlive":true,"SeqNumb":3019}}
 *
 * body is Historian payload: {"d":{"gwName":"RG120ABB02","devName":"WM_01_TREND","TIMESTAMP":37545,"ARRIVALTIME":0,"CASING":177.4455,
 * "TUBING":151.3181,"LINE":130.9298,"FLOWRATE":304.1823,"devIsAlive":true,"SeqNumb":12}}
 *
 * It internally calls processGatewayMessage() API that will store the data and timestamp it to LiveDataAccessService.
 */
var processDataPayload = function(body) {
    console.log(getCurrentDateTime(), ' General Payload from redigateMessage received: -----------------');
    var x;
    var y;
    var jsonPayLoad = {};
    var devInstanceValue = '';
    var appInstanceValue = '';
    var timeStampValue = '';
    var tagArray = [];
    var app, array, reg;
    for (y in body) {
        for (x in body[y]) {
            var key = x;
            var val = body[y][x];
            if (key == 'devName') {
                appInstanceValue = val.toString().trim();
            } else if (key == 'gwName') {
                devInstanceValue = val.toString().trim();
                val = '';
            } else if (key == 'TIMESTAMP') {
                timeStampValue = val;
                val = '';
            } else if (key == 'SeqNumb') {
                val = '';
            } else if (key == 'devIsAlive') {
                val = '';
            } else {
                let tmpname = key.toString().trim();
                let cycleArray = tmpname.split('_');
                if (cycleArray.length > 4) {
                    if (cycleArray[2] == "APP") {
                        app = val.toString().trim();
                    } else if (cycleArray[2] == "ARRAY") {
                        array = val.toString().trim();
                    } else if (cycleArray[2] == "REG") {
                        reg = val.toString().trim();
                    }
                    if (app && array && reg) {
                        var tag = {
                            Name: "LOG_REG_" + cycleArray[3],
                            Value: app + "." + array + "." + reg
                        };
                        tagArray.push(tag);
                    }
                } else {
                    var tag = {
                        Name: key.toString().trim(),
                        Value: val.toString().trim()
                    };
                    tagArray.push(tag);
                }
            }
        }
    }

    if (appInstanceValue.toUpperCase() === "CELLMODEM") {
        isLastMessageProcessed = true;
        console.log(getCurrentDateTime(), "AppInstance not supported as of now.");
    } else {
        jsonPayLoad = {
            devInstance: devInstanceValue,
            appInstance: appInstanceValue,
            tag: tagArray
        };

        var devNameSplited = body.d.devName.split("_");
        var gatewayMethod = "";
        var requestMethod = "POST";

        if (parseInt(devNameSplited[2]) === 7) {
            console.log(getCurrentDateTime(), ' Trend data from redigateMessage received: =======================');
            gatewayMethod = historianMessageAPI;
            jsonPayLoad.timestamp = timeStampValue.toString().trim();
            requestMethod = "POST";
        } else if (parseInt(devNameSplited[2]) === 8) {
            console.log(getCurrentDateTime(), ' AlarmLog data from redigateMessage received: =======================');
            gatewayMethod = alarmLogAPI;
            requestMethod = "POST";
        } else {
            console.log(getCurrentDateTime(), ' Live data from redigateMessage received: =======================');
            gatewayMethod = gatewayMessageAPI;
            requestMethod = "PUT";
        }

        processGatewayMessage(jsonPayLoad, gatewayMethod, requestMethod, function(output) {
            isLastMessageProcessed = true;
            console.log(getCurrentDateTime(), "Call to " + gatewayMethod + " API's is Completed");
        });
    }
};

/**
 * This function Loop the key/vals and ignore the non-data items. Map gwName to <RMC ID>, devName to <APP ID>
 * @param {string} : body -- This payload contains deviceName, appName, tagName and tagValue
 *
 * Birth payload: {"d":{"gwName":"RG400ABB01","Tarball_Date":"2017-08-24-1000","MQttBroker_IP":"127.0.0.1","MQtt_NumbConnects":4,"Numb_Devices":1,
 * "Device[0]_Name":"ITOTALFLOW","ACE_Config_XML_Info":"dc06aaf0e55e773efe2a8b5fe503160e  /usr/director/config/LighthouseMVP-VSlice1-RG400-V2.xml.gz",
 * "ACE_Config_UFF_Info":"87761948e7dcc56864a10f7b83df45a4  /usr/director/config/RG-400E.uff","Gateway_Uptime":"00:49:28 up 55 min, 
 * load average: 0.41, 0.53, 0.61","Free_Memory":"Mem: 253988 24152 229836 0 512","Disk_Usage":"ubi0:rootfs 57476 22172 35304 39% /",
 * "Gateway_Time":"1999-11-30T00:49:28.482","MQtt_Msgs_Recv":6,"MQtt_Msgs_Sent":122,"Connection":"ONLINE","SeqNumb":0}}
 *
 * It internally calls processGatewayMessage() API that will store the data and timestamp it to LiveDataAccessService.
 */
var processBirthPayload = function(body) {
    console.log(getCurrentDateTime(), ' Birth payload from RedigateMessage received: -----------------');
    isDeviceOffline = false;

    var jsonPayLoad = {
        devInstance: body.d.gwName.toString().trim(),
        connectionStatus: "Connected"
    };

    processGatewayMessage(jsonPayLoad, DeviceStatusAPI, "POST", function(output) {
        isLastMessageProcessed = true;
        console.log(getCurrentDateTime(), "Call to " + DeviceStatusAPI + " API's is Completed");
    });
};



/**
 * This function process the device message and check whether its data message, trend message or 
 * birth payload message and delete it from service queue once it done processing
 * @param {Object} : sbService -- Instance of Service Queue
 * @param {Object} : err -- Holds error message
 * @param {function} : redigateMessage -- Service Queue json message i.e. device data
 */
function processMessage(sbService, err, redigateMessage) {
    if (err) {
        if (err == 'No messages to receive') {
            console.log(getCurrentDateTime(), err);
        } else {
            console.error(getCurrentDateTime(), 'Error on Rx: ', err);
            console.error(getCurrentDateTime(), 'Error Paylaod: ', redigateMessage);
        }
        isLastMessageProcessed = true;
    } else {
        if (redigateMessage.body != "OFFLINE") {
            try {
                var body = JSON.parse(redigateMessage.body);
                console.log(getCurrentDateTime(), 'Redigate Device Message: ', body);
                if (body.d.Tarball_Date == undefined) {
                    if (body.d.devIsAlive === true || body.d.devIsAlive == undefined) {
                        console.log(getCurrentDateTime(), ' Message payload in redigateMessage received: =======================');
                        if(isDeviceOffline || body.d.devIsAlive == undefined){
                            processBirthPayload(body);
                        }
                        processDataPayload(body);
                    }
                    
                } else {
                    console.log(getCurrentDateTime(), ' Birth payload in redigateMessage received: =======================');
                    processBirthPayload(body);
                }
            } catch (err) {
                console.error(getCurrentDateTime(), ' Invalid paylaod payload: ', err);
                isLastMessageProcessed = true;
            }
        } 
    }
}

/**
 * This function receives service queue messages
 * @param {Object} : sbService -- Instance of Service Queue
 * @param {string} : queueName -- Service bus queue name
 * @param {function} : callback -- It's a callback function which will be called on message receive or error while receiving queue message
 */
function checkForMessages(sbService, queueName, callback) {
    console.log(getCurrentDateTime(), 'Heart beat: check for Messages every second in queue: ' + queueName + ". isLastMessageProcessed: " + isLastMessageProcessed);
    if (isLastMessageProcessed) {
        isLastMessageProcessed = false;

        sbService.receiveQueueMessage(queueName, function(err, redigateMessage) {
            if (err) {
                callback(err);
            } else {
                console.log(getCurrentDateTime(), 'Redigate Complete Payload: ', JSON.stringify(redigateMessage));
                if (redigateMessage && redigateMessage.body) {
                    //callback(null, redigateMessage);  BB:
                    isLastMessageProcessed = true;  //BB: stop here for testing ServiceBus and Webjob functional.
                } else {
                    var errorMessage = 'Body missing in payload';
                    callback(errorMessage, redigateMessage);
                }
            }
        });
    }
}

/**
 * This function reads message count from service bus queue
 * @param {string} : queueName -- Service bus queue name
 */
function checkMessageCount(queueName) {
    sbService.getQueue(queueName, function(err, queue) {
        if (err) {
            console.error(getCurrentDateTime(), 'Error on get queue length: ', err);
        } else {
            var length = queue.CountDetails['d2p1:ActiveMessageCount'];
            console.log(getCurrentDateTime(), ' current messages count in the queue: ', length);
            return length;
        }
    });
}



// BB: devConnStr and queueName must be a parameter as they are unique to each installation.
// devConnStr is the connection string to the Service Bus Namespace.
var connStr = process.env.SERVICE_BUS_CONNECTION_STRING; // BB: || devConnStr;
var queueName = process.env.SERVICE_QUEUE; //BB: || 'servicebusqueue1-acme2';

console.log(getCurrentDateTime(), 'Connecting to ' + connStr + ' and queue ' + queueName);

var sbService = azure.createServiceBusService(connStr);

// BB Todo: CreateQueue... can create a queue that IoT Hub is not routed to.
sbService.createQueueIfNotExists(queueName, function(err) {
    if (err) {
        console.error(getCurrentDateTime(), 'Failed to create queue: ', err);
    } else {
        isLastMessageProcessed = true;
        setInterval(checkForMessages.bind(null, sbService, queueName, processMessage.bind(null, sbService)), 2000);
        //setInterval(checkMessageCount.bind(null, queueName), 1000);
    }
});