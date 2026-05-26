Firehose
========


A high performance Business Rules Engine in Go. Supporting Caching, Running Once, Throttling, Debouncing, Logging, Tracing, Monitoring and many more. Zero memory allocation.


# Problem

Many systems are modeled around the idea of reacting to events such as (HTTP requests, GRPC request, Kafka message, RMQ message...etc). They have preconditions and side effects that needs to be applied to the system. 
This pattern when not enforced leads to many tight coupling and the system becomes hard to maintain and very brittle when changing. Instrumenting applications are manual in most of the cases because the number of different concepts in the system doesn't allow for a generalization. Also the cost of introducing parallelism and distributing computation becomes a headache.

# Goals

* Limit the number of system primitives to control the overall complexity
* Define a clear pattern for all events processing sync/async 
* Allow Developers to define logic in isolation instead of iterating over the same piece of code 
* Come up with a DSL that makes it easy to communicate the system behavior between Developers and Product Managers
* Allow for component re-usability in the system and between teams that uses the same package

# High-level Concepts

We could imagine our systems follows: 

- Source: A source of event. like HTTP server, Kafka consumer, Redis Set consumer, Filesystem watcher
- Precondition: A condition that determine of the logic ahead should be executed for this event ("valid = true", "has_profanity = true", "blocked = false")
- Action: A component takes the event as input and produce an ourput the represent the side effect on the system (Convert a twitch message to block event, Reduce the size of large input to just the needed attributes)
- Destination: A component that will apply the side effect to the system or external system (Writing to DB, Kafka, Redis, HTTP response).

A combination of these 4 components will be called "Rule" (As in Business Rule). 

Example:

- HTTP request to get list of customer payment methods (source), Validate the customer is logged in (precondition), Get the list of payment methods for this customer and return a list (Action), Write the list as a JSON response (Dest).
- Consumer Kafka topic for User information (Source) if the message is valid (Precondition), Write it to database table "users" (Destination)


Organizing our system in such pattern will allow for wrapping the Rule concept in different wrappers that adds a functionality. the Rule definition decides if the feature is on or off. 

Example:

- Logging: logs the rule execution status (success, error, skip...etc)
- Metrics: Report Prometheus metrics
- Tracing: Report OpenTelemetry tracing for each rule and part of the rule. 
- Caching: Decides when to cache and if to read from the cache
- Panic recovery: limit the impact of panics to specific business rule. 
- Execute once: make sure the rule is executed once per input.


# Execution Engine

An engine should allow for: 
- Defining the rules in DSL format
- Add/Remove/Update rules in Runtime
- Fan-out events to rules using same source
- Allow for wrapping the rules with extra logic and features 
- Responsible for safely executing each rule without degrading the whole system if 1 rule if faulty.
- Communication with other instances to distribute workload


# Non-functional Goals

- Zero heap allocation as much as we can
- Maximum golangci-lint configuration
- Minimum dependencies
- Minimum exported interface
- Rule are data not code


# Usecases

- Web service that reacts to HTTP calls and do several actions (CRUD)
- Kafka consumer/producer, multiple consumer groups/topics. 
- ETL processor from SQS/DB/Redis/....
- Game engine


# How to use 

``` go
import "github.com/emad-elsaid/firehose"

func main() {
	ctx := context.Background()
	err := firehose.AddRule(ctx, printTime)
	if err != nil {
		panic(err)
	}

	firehose.Start(ctx)
}

var printTime = firehose.Rule[events.Time, events.Time]{
	When: sources.Time{Period: 1 * time.Second},
	Then: actions.Yield[events.Time]{},
	To:   destinations.Stdout[events.Time]{},
}
```
