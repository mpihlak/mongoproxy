So, we need something that sits in front of MongoDb and analyzes the traffic
between the application and the database. Answer questions, such as:

 * What applications use the database ($application, IP address)
 * What collections are being accessed and how
 * How many bytes are passed back and forth
 * and more

Basically have a configuration on what to listen to and where to forward the
queries.

Reveive a packet, parse it's content, increase stats and pass it on to the
destination. Expose the statistics over a metrics endpoint.

Stage 1:
Single thread
Listen on a socket
Parse incoming messages
Print them out
