msgpack-delphi
==============
From http://www.msgpack.org/
> MessagePack is an efficient binary serialization format. It lets you exchange data among multiple languages like JSON but it's faster and smaller.
> For example, small integers (like flags or error code) are encoded into a single byte, and typical short strings only require an extra byte in addition to the strings themselves.
> 
> If you ever wished to use JSON for convenience (storing an image with metadata) but could not for technical reasons (encoding, size, speed...), MessagePack is a perfect replacement.

This Delphi-FPC implementation aims to be simple but still have good speed.

* single unit
* ~1350 lines
* simple streaming suport (any TStream)
* no external dependencies
* load/parse ~2 times faster than equivalent JSON with superobject

It also uses Interfaced objects to provide a simple Garbage Collector mechanism, so no need to call .Free or handle cloning manually in your program.

Specification conformance
--------

It supports the latest msgpack 2.0 specification except ext type (Extension type).

Map Keys
--------

MsgPack has no limitations for the type of map keys, they can be anything including other maps. The unit has two configurations defined with the *STRINGMAPKEYS* conditional directive at the top of the unit.

* STRINGMAPKEYS Enabled: Maps allows only bytes of string keys. An exception is raised if something else is found as a key. Map operations are usually 2x faster.
* STRINGMAPKEYS Disabled: Map keys can be any type.

Works with
--------

* Delphi 7 (tested)
* Delphi 2009-XE (not tested)
* Delphi XE2 (tested)
* Delphi XE3-5 (not tested)
* FPC (tested)

Included
--------

* Benchmark against Json (using superobject)
* Very simple test program.

To-Do
--------

Documentation

Comprehensive tests
