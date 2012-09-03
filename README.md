msgpack-delphi
==============
> MessagePack is an efficient binary serialization format. It lets you exchange data among multiple languages like JSON but it's faster and smaller.
> For example, small integers (like flags or error code) are encoded into a single byte, and typical short strings only require an extra byte in addition to the strings themselves.
> 
> If you ever wished to use JSON for convenience (storing an image with metadata) but could not for technical reasons (encoding, size, speed...), MessagePack is a perfect replacement.

This Delphi-FPC implementation intent to be:

* Simple
* Lightweight
* Fast

Right now the code is really simple (around 1000 lines) and have a very good speed (2 to 3 times faster than superobject).

It uses Interfaced objects to provide a simple Garbage Collector mechanism, so no need to call .Free in your program.

Works with
--------

* Delphi 7 (tested)
* Delphi XE2 (tested)
* Delphi 2005 to Delphi XE (should work)
* FPC (should work)

Included
--------

* Benchmark against Json (using superobject)
* Very simple test program.

To-Do
--------

Documentation