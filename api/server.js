
var redis = require("../node_redis/"),
    client = redis.createClient();

client.on("error", function (err) {
    console.log("Redis connection error to " + client.host + ":" + client.port + " - " + err);
});

client.set("string key", "string val", redis.print);
client.hset("hash key", "hashtest 1", "some value", redis.print);
client.hset(["hash key", "hashtest 2", "some other value"], redis.print);
client.hkeys("hash key", function (err, replies) {
    console.log(replies.length + " replies:");
    replies.forEach(function (reply, i) {
        console.log("    " + i + ": " + reply);
    });
    client.quit();
});

var journey = require('../journey/lib/journey');

//
// Create a Router object with an associated routing table
//
var router = new(journey.Router)(function (map) {
    map.root.bind(function (res) { res.sendBody(JSON.stringify({hello:"world"})); } );
    map.get(/^conversation\/([0-9]+)$/).bind(function (res, id) {

    });
    map.post('/conversations').bind(function (res, data) {
        sys.puts(data.type); // "Cave-Troll"
        res.send(200);
    });
});

require('http').createServer(function (request, response) {
    var body = "";

    request.addListener('data', function (chunk) { body += chunk });
    request.addListener('end', function () {
        //
        // Dispatch the request to the router
        //
        router.route(request, body, function (result) {
            response.writeHead(result.status, result.headers);
            response.end(result.body);
        });
    });
}).listen(8080);
