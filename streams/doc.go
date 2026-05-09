// Package streams is holocron's stream processing library — a small
// Kafka Streams analog. It composes producers, consumers, and state
// stores into a topology so stateful event-driven applications can be
// expressed as configuration of operators rather than as bespoke
// goroutines and state-management code.
//
// The DSL:
//
//	top := streams.New(transport)
//	top.Stream("clicks").
//	    Filter(func(r proto.Record) bool { return len(r.Value) > 0 }).
//	    Map(func(r proto.Record) proto.Record { ... }).
//	    To("clicks-cleaned")
//
//	top.Stream("orders").
//	    GroupByKey().
//	    Count("order-counts").
//	    To("counts-out")
//
//	top.Run(ctx)
//
// streams imports only sdk + proto. Like connect, it is a layer above
// the broker that uses the SDK exactly as any user would. A topology
// runs against an inproc Transport (tests, demos) or a network
// Transport (production deployments) with no code change.
package streams
