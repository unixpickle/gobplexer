# gobplexer

So you have a TCP socket, and you want to send objects over it. Easy enough: just use [Gob](https://golang.org/pkg/encoding/gob/). But now, suppose you want to multiplex this stream of objects in a dynamic way. For instance, if you are making a messaging app, you might want a stream of messages for each chat. You *could* manually multiplex the stream of objects, reading them from the socket and sending different objects to different chats. However, this introduces some overhead: now the code reading from the TCP socket needs to be able to distribute objects to different places. It would be much easier if there was some abstraction to multiplex the stream of objects.

With gobplexer, it is possible and easy to multiplex a bidirectional stream of objects. Even better, it is even possible to multiplex a multiplexed stream, and even to multiplex a multiplexed multiplexed stream.
