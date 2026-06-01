# RabbitMQ Advanced Demo

A complete RabbitMQ training project with .NET 8, designed as a professional portfolio sample.

## Day 1 – Basic Producer and Consumer Implementation

### Completed Tasks

* Set up RabbitMQ using Docker
* Created a Solution and two Console projects
* Implemented simple message publishing and consumption using a Queue

### How to Run

1. Start RabbitMQ with Docker.
2. Run the Consumer project.
3. Run the Producer project.

## Day 2 – Queues, Exchanges, and Direct Exchange

### Implemented

* Defined a Direct Exchange
* Created durable queues
* Configured bindings with routing keys
* Published messages based on different routing keys
* Consumed messages through separate consumers

### Key Concepts Learned

* The difference between a Queue and an Exchange
* How a Direct Exchange works
* Bindings and Routing Keys

## Day 3 – Fanout Exchange and the Publish/Subscribe Pattern

### Implemented

* Implemented a Fanout Exchange
* Broadcast messages to multiple queues
* Created multiple consumers for a single exchange
* Applied the Publish/Subscribe messaging pattern

### Key Concepts

* The difference between Fanout and Direct Exchanges
* Broadcast use cases (Notifications, Logging, Analytics)

## Day 4 – Topic Exchange and Advanced Routing

### Implemented

* Implemented a Topic Exchange
* Used wildcard patterns (`*` and `#`)
* Designed meaningful routing keys
* Configured bindings with different routing patterns

### Key Concepts

* The advantages of Topic Exchange over Direct and Fanout Exchanges
* Applications in Event-Driven Systems

## Day 5 – Publisher Confirms

### Implemented

* Enabled Publisher Confirms
* Used `ConfirmSelect()` and `WaitForConfirms()`
* Verified successful or failed message delivery
* Improved producer reliability

### Key Concepts

* Guaranteeing message delivery to RabbitMQ
* Handling network and broker failures
* The difference between Synchronous and Asynchronous Confirms

## Day 6 – Consumer Acknowledgements (Ack/Nack)

### Implemented

* Manual Acknowledgement and Auto Acknowledgement
* `BasicReject` and `BasicNack`
* Message requeueing on processing failures
* Consumer-level error handling

### Key Concepts

* The importance of Manual Acknowledgements in production environments
* Preventing message loss
* Controlling processing flow and implementing basic retry mechanisms

## Day 7 - Dead Letter Exchange (DLX) and Dead Letter Queue

### Completed:

* Configured a Dead Letter Exchange (DLX) and Dead Letter Queue (DLQ)
* Configured the main queue with the `x-dead-letter-exchange` argument
* Routed failed messages to the DLQ using `BasicNack` with `requeue: false`
* Implemented dead-letter message handling

### Key Concepts:

* Preventing message loss
* Isolating problematic messages from normal processing
* Establishing the foundation for a Retry Mechanism

## Day 8 - Retry Mechanism with Dead Lettering and Backoff

### Completed:

* Implemented a retry mechanism using TTL and Dead Letter Exchange (DLX)
* Managed retry attempts using a Retry Count header
* Implemented a basic backoff strategy with a 10-second delay
* Moved messages to the Dead Letter Queue (DLQ) after exhausting all retry attempts

### Key Concepts:

* Retry pattern with Dead Lettering
* Preventing message loss
* Backoff strategy for controlled retry intervals

## Day 9 - Message TTL, Queue TTL, and Expiration

### Completed:

* Configured Message TTL at both the per-message and queue levels
* Configured Queue Expiration using the `x-expires` argument
* Applied message-specific expiration using the `properties.Expiration` property
* Managed expired messages and their lifecycle within RabbitMQ

### Key Concepts:

* Preventing the accumulation of stale messages
* Efficient resource and memory management
* TTL configuration at both the Queue and Message levels
* Automatic cleanup of inactive queues and expired messages
