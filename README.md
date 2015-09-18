# Marshal - a Kafka consumer coordination library

[![GoDoc](http://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/zorkian/marshal)
[![Build Status](https://travis-ci.org/zorkian/marshal.svg)](https://travis-ci.org/zorkian/marshal)

Caveat lector: This is under construction. It's not really ready for use, and is certainly
not yet battle tested in production.

## Purpose

This project assumes you have some familiarity with Kafka. You should know what a topic is and
what partitions are.

In Kafka, the unit of scalability is the partition. If you have a topic that is getting "too
busy", you increase the partition count. Consumption of data from those busy topics requires
consumers to be aware of these partitions and be able to "coordinate" their consumption.

Traditional setups use Zookeeper or some other system for coordinating consumers. This works
in many situations, but introduces a level of complexity that isn't necessary. If Zookeeper is
down, but Kafka is up, can your consumers still run? Do they shut down? Why have that extra
dependency when coordination can be managed 100% within Kafka?

Additionally, getting consumer coordination correct is a rather taxing exercise of development
and, frankly, shouldn't need to be done for every single project, company, etc.

Marshal is a library that you can drop into your Go programs and use it to coordinate the
consumption of partitions across multiple processes, servers, etc. It is implemented in terms
of Kafka itself: no extra dependencies.

Marshal is designed for use in production environments where there are many topics, each topic
having hundreds of partitions and potentially thousands of consumers working in concert
across the infrastructure to consume them.

## Usage

This module is designed to be extremely simple to use. The basic logical flow is that you
create a Marshaler and then you use that to create as many Consumers as you need topics to
consume. Logically, you want one Marshaler globally, and you want a single Consumer per topic
that you need to consume from.

Here's the simplest example:

```go
package main

import "fmt"
import "github.com/zorkian/marshal/marshal"

func main() {
    marshaler, _ := marshal.NewMarshaler(
        "clientid", "groupid", []string{"127.0.0.1:9092"})
    defer marshaler.Terminate()

    consumer, _ := marshal.NewConsumer(marshaler, "some-topic", marshal.CbAggressive)
    defer consumer.Terminate()

    for {
        fmt.Printf("Consumed message: %s", consumer.Consume())
    }
}
```

That's it. Please see the rest of the documentation to make sure you understand what is going
on behind the scenes.

## Important Notes

This system assumes that timestamps are valid. If your machines are not using NTP to synchronize
their clocks, you will not be able to get deterministic behavior. Sorry.

Marshal also relies on all actors being good actors. Malicious users can cause the system to act
unpredictably or at their choosing.

## Consumer Coordination

This section discusses the way that Marshal handles coordination internally. Please read and
understand this, as it is relevant to how you deploy and use this library.

TBD.

## Bugs

They are legion. This is a new project. There are tests, however, and we very much welcome
the submission of bug reports, pull requests, etc.

https://github.com/zorkian/marshal
