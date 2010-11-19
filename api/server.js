
var redis = require("../lib/node_redis"),
    client = redis.createClient();

client.on("error", function (err) {
  console.log("Redis connection error to " + client.host + ":" + client.port + " - " + err);
});

var express = require('../lib/express');
var app = express.createServer();

app.get('/', function(req, res){
    res.send('Hello World');
});

app.get('/conversation/:id', function (req, res, next) {
    // Grab the conversation from the db
    var id = req.params.id;
    if (id) {
        client.zrange("conversation:" + id + ":messages", 0, -1, function (err, messages) {
            console.log(" messages: " + messages.length);
            console.log("messages: " + messages);
            msgs = new Array(messages.length);
            messages.forEach(function(message, i) {
                console.log("message: " + message);
               msgs.push(message + ""); 
            });
            res.send(msgs);
        });
    } else {
        next(new Error('Failed to load user ' + req.params.id));
    }
});

app.listen(8080);

