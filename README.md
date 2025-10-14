# 📨 HermesMQ

HermesMQ is a **lightweight, high-performance message queuing system** built around the **Publish/Subscribe (Pub/Sub)** architecture. 

I found **Kafka** super interesting after using it in a previous project, and wanted to try building my own messaging system from scratch.

Run a broker:
```
go run cmd/broker
```

Run the client and interact with the broker:
```
go run cmd/client
```

The interface to interact:
```
➜ HermesMQ git:(master) ✗ go run .\cmd\client\
2025/10/14 10:32:34 ✅ connected to broker

📖 HermesMQ Client Commands:
  sub <topic>           - Subscribe to a topic
  unsub <topic>         - Unsubscribe from a topic
  pub <topic> <message> - Publish a message to a topic
  list                  - List subscribed topics
  help                  - Show this help
  quit                  - Exit client

> sub test-topic
✅ subscribed to 'test-topic'
> 2025/10/14 10:32:39 received payload from topic: Successfully subscribed!
```

> [!NOTE]
> I will be making a dashboard to see live connections and to manage topics soon!
