# Marshal - a Kafka consumer coordination library

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/zorkian/marshal)
[![Build Status](https://travis-ci.org/zorkian/marshal.svg)](https://travis-ci.org/zorkian/marshal)

Caveat lector: This is under construction. It's not really ready for use, and is certainly
not yet battle tested in production.

## Purpose

Busy topics have many partitions, and coordinating N consumers of those partitions is a somewhat
non-trivial task. Most people use Zookeeper or write some other coordination system with the
logic on their own, but getting everything right is quite taxing.

Marshal is a library that you can drop into your Go programs and use it to coordinate the
consumption of partitions across multiple processes, servers, etc. It is implemented in terms
of Kafka itself: in other words, Marshal uses a Kafka topic to coordinate.

This provides a lot of benefit -- not least of which is that Kafka can handle a lot more
traffic than Zookeeper. It also simplifies failure management by a lot, since "can't talk to
Kafka" means you can neither heartbeat nor consume.

Marshal is designed for use in production environments where there are many topics, each topic
having hundreds of partitions, and potentially thousands of consumers working in concert
across the infrastructure.

## Usage

TBD. For now, see examples. If I've written them yet.

## Notes

This system assumes that timestamps are valid. If your machines are not using NTP to synchronize
their clocks, you will not be able to get deterministic behavior. Sorry.

Marshal relies on all actors being good actors. Malicious users can cause the system to act
unpredictably or at their choosing.

## Bugs

They are legion. This is a new project. There are tests, however, and we very much welcome
the submission of bug reports, pull requests, etc.

https://github.com/zorkian/marshal
