---
layout: page
title: Provider Model
---

# Provider Model

Some features of Orleans leverage external storage and messaging systems for persisting runtime or application state.
Orleans integrates with such external systems via its provider model.
Each feature area that supports providers defines its own provider API.
A module that implements that API, a provider, can be added to a silo or a client via the `SiloHostBuilder` and `ClientBuilder` APIs respectively and configured via its options class.

Most provider categories are used only in a silo, but some are used for both silos and clients.
An example of a system feature is the Orleans clustering protocol, for which a provider is required on both silo and client side.
On the other hand, a reminder provider is optional (only needed if grains use reminders), and can only be added to a silo.

Here's a list of provider categories and their purposes.

| **Category** | **Purpose** | **Silo/Client** | **Required** |
|----------|-----------|--------------|-------------|-----------|
| [Clustering](Clustering/Clustering-Overview.md) | Storage for cluster state data | Both | Yes |
| [Reminders]( Reminders/Reminders-Overview.md) | Storage for reminder state | Silo | No |
| Persistence | Storage for grain state | Silo | No |
| Streaming | Adapter for message queues | Both | No |
| Hosting | Adapter for integrating with hosting environment  | Both | No |
| Telemetry | Adapter for sending telemetry to monitoring systems | Both | No |
| Serialization | Serializers for sending data over the network  | Both | No |
