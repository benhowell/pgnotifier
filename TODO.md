# pgnotifier
A simple little module to capture, process, and dispatch Postgresql NOTIFY streams
<br>
## TODO
A list of stuff to look into at a later date.

* Type annotations for function sigs
* Better exception handling
  * There is very little in the way of catching possible exceptions.
  * Custom exceptions to give salient information.
  * User settings to configure if/how exceptions are handled (global,
  as function params, whatever makes sense)
* SYSCHAN needs further consideration. I expect a future release to
properly integrate the SYSCHAN with the functionality of a regular
channel with the exception that it is always on the listener thread.
Perhaps system commands can be executed via messages from the channel?
Postgresql system messages? Other non-application level stuff?
* Tests
