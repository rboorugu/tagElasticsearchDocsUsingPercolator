var schedule = require('node-schedule');

var rule = new schedule.RecurrenceRule();
rule.hour = 04;
rule.minute = 30;

var j = schedule.scheduleJob(rule, function(){
    var tagger = require("./tagDocuments"),
        config = require("./config");
        
    var filterDateString = null;
    if (config.onSchedule == true) {
        filterDateString = new Date(getDate()).toISOString().substring(0, 10);
    }
    else if(config.tagDocsAfterDate) {
        filterDateString = new Date(config.tagDocsAfterDate).toISOString().substring(0, 10);
    }
    else{
        filterDateString = new Date();
    }
    
    var exit = function() {
        setTimeout(function() {
            process.exit();0
        }, 10000);
    }
   
    var tagDocs = function myself(index) {
        if (index >= 0) {
            tagger.start(function() { myself(index-1) }, filterDateString, config.docTypesToTag[index]);
        }
        if (index == 0 && config.shutdownOnComplete) {
            exit();
        }
    }

    if (config.docTypesToTag) {
         console.log("tagging docTypesToTag after:" + filterDateString);
        tagDocs(config.docTypesToTag.length-1);
    } else {
        console.log("tagging after:" + filterDateString);
        console.log("before tagger.start");
        console.log(filterDateString);
        tagger.start(exit, filterDateString, null);
    }
});

function getDate() {

    var today = new Date();
    var mm = today.getMonth()+1; //starts with 0 which is Jan
    var dd = today.getDate()-1;

    var yyyy = today.getFullYear();
    if(dd<10){
        dd='0'+dd
    }
    if(mm<10){
        mm='0'+mm
    }
    return mm+'/'+dd+'/'+yyyy;

}

