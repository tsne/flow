// Package flow provides a message streaming framework with pluggable pub/sub
// system and an optional storage. The message passing and distribution is
// managed by brokers which are clustered into groups. A group can be manually
// assigned to a broker. If none is set, a default group will be used. Brokers
// within one group are arranged on a ring where each broker has its own node key
// assigned. All incoming messages use a partition key to decide which broker
// in the group is responsible for which message.
//
// When a broker joins a group a message is sent to the other group members to
// inform about the new node. Likewise a message is sent when a broker leaves
// the group. So each broker is able to build its own local view of the group.
// In order to keep this view up to date ping messages are sent to the other group
// members. So it possible to detect when a node dies unexpectedly.
package flow
