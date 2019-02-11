// Package flow provides a message streaming framework with pluggable pub/sub
// system. The message passing and distribution is managed by brokers which are
// clustered into cliques. A clique can be manually assigned to a broker. If
// none is set, a default clique will be used. Brokers within the same clique
// are arranged in a ring structure where each broker has its own node key
// assigned. All incoming messages use a partition key to decide which broker
// in the clique is responsible for which message.
//
// When a broker joins a clique a message is sent to the other clique members to
// inform about the new node. Likewise a message is sent when a broker leaves
// the clique. So each broker is able to build its own local view of the clique.
// In order to keep this view up to date ping messages are sent to the other clique
// members. So it possible to detect when a node dies unexpectedly.
package flow
