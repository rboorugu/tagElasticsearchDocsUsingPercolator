/**
 * Created by Rupesh on 12/01/2016.
 */
var DocumentTagger = (function () {
    var config = require('./config'),
        es = require("elasticsearch"),
        async = require("async"),
        _ = require("underscore"),
        //stats = require("./stats"),
        client = new es.Client(config.es.config),
        batchSize = config.batchSize,
        queueConcurrency = config.queueConcurrency,
        query = null,
        totalBatchCount = 0

    return {
        start: function (onComplete, tagDocsAfterDate, docType) {
            var cnt = 0;
            var filters = [];
            if (tagDocsAfterDate) {
                var dateFilter = {
                    range: {
                        ElasticImportDate: {
                            gte: tagDocsAfterDate
                        }
                    }
                };
                filters.push(dateFilter);
            }
            console.log(docType);
            if (docType) {
                var docTypeFilter = {term: {DocumentType: docType}};
                filters.push(docTypeFilter);
            }
           
            if (filters.length === 0) {
                query = {
                    query: {
                        match_all: {}
                    }
                };
            }
            if (filters.length === 1) {
                query = {
                    bool: {
                        must: {
                            match_all: {}
                        },
                        filter: filters[0]
                    }
                };
            } else if (filters.length > 1) {
                query = {
                    bool: {
                        must: {
                            match_all: {}
                        },
                        filter: {
                            bool: {
                                must: filters
                            }
                        }
                    }
                };
            }

            var request = {
                body: {
                    aggs: {
                        universalIds: {
                            scripted_metric: {
                                init_script: "params._agg['ids'] = []",
                                map_script: "params._agg.ids.add(doc['id'].value)"
                            }
                        }
                    },
                    query: query,
                    stored_fields: [],
                },
                size: 0,
                index: config.es.index,
                type: config.es.type
            };

            client.search(request).then(function (resp, error) {
                if (error) {
                    console.log("Error retrieving document ids.", {error: error});
                } else {
                    return _.flatten(resp.aggregations.universalIds.value.map(function (ids) {
                        return ids.UniversalIds;
                    }));
                }
            }).then(function (universalIds) {
                if(!universalIds || universalIds.length == 0) {
                    console.log("No documents found to tag.");
                    onComplete();
                } else {
                    universalIds = universalIds.sort(function (a, b) {
                        return a - b;
                    });
                    //console.log(universalIds);
                    console.log("Retrieved " + universalIds.length + " ids.");
                    totalBatchCount = Math.round(universalIds.length/batchSize);
                    console.log("Total batch count " + totalBatchCount);
                    var batchNumber = 0;
                    var x = 0;
                    var batchItems;
                    while (x < universalIds.length) {
                        batchNumber += 1;
                        batchItems = universalIds.slice(x, x + batchSize);
                        x += batchSize;
                        if (batchNumber % 5000 === 0) {
                            console.log("Queuing batch: " + batchNumber);
                        }
                        queue.push({
                            items: batchItems,
                            batchNumber: batchNumber
                        }, processedMessage(batchNumber,batchSize));
                    }
                    console.log("Done creating " + batchCount + " batches of " + batchSize + " items.");
                }
            });

            var queue = async.queue(function (batch, callback) {
                var dt = new Date()
                console.log("Queuing : " + batch.batchNumber + " processed at " + new Date().toLocaleString());
                //console.log(batch.items);
                var searchRequests = [];
                batch.items.forEach(function(batchItem){
                    searchRequests.push(JSON.stringify({"index" : "search-stack"}));
                    searchRequests.push(JSON.stringify({
                        "from": 0,
                        "size": 10000,
                        "query": {
                            "percolate": {
                                "field": "query",
                                "document_type": config.es.type,
                                "index": config.es.index,
                                "type": config.es.type,
                                "id": batchItem
                            }
                        },
                        "_source": false
                    }));
                })
                var searchRequestBody = {body:[]};
                searchRequestBody.body.push(searchRequests.join(',\n'));

                client.msearch(searchRequestBody).then(function(resp, error){
                   if (error){
                       console.log("Failed at search - batch " + batch.batchNumber + ". Will requeue batch.");
                       console.log(JSON.stringify(err, null, 2));
                       queue.push({
                           items: batch.items,
                           batchNumber: batch.batchNumber
                       }, processedMessage(batchNumber,batchSize));
                   } else {
                       var updateRequestBody = []
                       for (var respLoop = 0; respLoop <= resp.responses.length - 1 ; respLoop++)
                       {
                           if (resp.responses[respLoop].hits.hits.length > 0)
                           {
                               //var temp = ids[respLoop];
                               var operation = {};
                               operation.update = {
                                   _index: "search-stack",
                                   _type: "all",
                                   _id: batch.items[respLoop]
                               }
                               updateRequestBody.push(JSON.stringify(operation))
                               updateRequestBody.push(JSON.stringify({
                                   doc: {
                                       Tags: resp.responses[respLoop].hits.hits.map(function(tag) {return tag._id})
                                   }
                               }));
                           }
                       }
                       //console.log(updateRequestBody.join('\n'))
                       if (updateRequestBody.length > 0)
                       {
                           client.bulk({
                               body: updateRequestBody.join('\n')
                           }, function (err, resp) {
                               if (err) {
                                   console.log("Failed at update - batch " + batch.batchNumber + ". Will requeue batch.");
                                   console.log(JSON.stringify(err, null, 2));
                                   queue.push({
                                       items: batch.items,
                                       batchNumber: batch.batchNumber
                                   }, processedMessage(batchNumber,batchSize));
                               } else {
                                   console.log("Updated documents - completed batchnumber " + batch.batchNumber   + " processed at " + new Date().toLocaleString()
                                       + "| Min Batch id: " + Math.min.apply(null,batch.items) + " Max Batch id: " + Math.max.apply(null,batch.items) + "| Total batches to be processed " + totalBatchCount);
                               }
                           });
                       }
                   }
                    callback();
                });
            }, queueConcurrency);

            queue.drain = function () {
                console.log('All batches have been queued. ' + new Date());
                onComplete();
            };
        }
    };
}());

module.exports = DocumentTagger;



var processedMessage =  function (batchNumber, batchSize) {
    return function () {
        console.log('Finished queuing batch ' + batchNumber + ' total document count ' + batchNumber * batchSize);
    };
}