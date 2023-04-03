
Please, use QueueRPC and QueueApplication by subclassing them.
This is the recommended way of using this library.

Here is class hierarchy and short description of what does what:

	    QueueBase			QueueLoopback
	/		\
QueueServer		QueueClient		
	|		|
QueueHandler		|
	|		QueueRPC
QueueApplication	|
	|		|
<your app>		<your client>


QueueBase handles common things between server and client.

QueueServer and QueueClient subclasses QueueBase and adds specific functions.

QueueHandler implements usefull features to add custom functionality
by subclassing, defining your own methods and publishing it on queue

QueueRPC provides client-side proxy-class, that, when subclassed and
provided with the list of published methods sends any calls to that
methods to queue for QueueHandler to handle

QueueAppliction subclasses QueueHandler and adds some additional features
for using it as a finished application with command line interface

QueueLoopback is for debugging purposes only and contains mock objects
that can be used to test library or your application without real network
connection to AMQP server

