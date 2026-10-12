//! # Async Channel
//!
//! In this exercise, you will use `tokio::sync::mpsc` async channels to implement producer-consumer pattern.
//!
//! ## Concepts
//! - `tokio::sync::mpsc::channel` creates bounded async channels
//! - Async `send` and `recv`
//! - Channel closing mechanism (receiver returns None after all senders are dropped)

use tokio::sync::mpsc;

/// Async producer-consumer:
/// - Create a producer task that sends each element from items sequentially
/// - Create a consumer task that receives all elements and collects them into Vec for return
///
/// Hint: Set channel capacity to items.len().max(1)
pub async fn producer_consumer(items: Vec<String>) -> Vec<String> {
    // `tx` is the Sender, `rx` is the Receiver. Capacity = items.len() (at least 1).
    let (tx, mut rx) = mpsc::channel(items.len().max(1));
    // Producer task: send every item, then `tx` is dropped so the channel closes.
    tokio::spawn(async move {
        for item in items {
            tx.send(item).await.unwrap();
        }
    });
    // Consumer task: receive until the channel closes, collecting into a Vec.
    let handle = tokio::spawn(async move {
        let mut results = Vec::new();
        while let Some(item) = rx.recv().await {
            results.push(item);
        }
        results
    });
    // Wait for the consumer task to finish and return its collected results.
    handle.await.unwrap()
}

/// Fan‑in pattern: multiple producers, one consumer.
/// Create `n_producers` producers, each sending `"producer {id}: message"`.
/// Consumer collects all messages, sorts them, and returns.
pub async fn fan_in(n_producers: usize) -> Vec<String> {
    // Capacity = number of producers (at least 1) so every message fits in the buffer.
    let (tx, mut rx) = mpsc::channel(n_producers.max(1));
    // Spawn one producer task per id; each clones its own sender.
    for id in 0..n_producers {
        let tx = tx.clone();
        tokio::spawn(async move {
            tx.send(format!("producer {id}: message")).await.unwrap();
        });
    }
    // Drop the original sender: the channel closes once all cloned senders finish.
    drop(tx);
    // Consumer (on the current task) receives until the channel closes, then sorts.
    let mut results = Vec::new();
    while let Some(item) = rx.recv().await {
        results.push(item);
    }
    results.sort();
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_producer_consumer() {
        let items = vec!["hello".into(), "async".into(), "world".into()];
        let result = producer_consumer(items.clone()).await;
        assert_eq!(result, items);
    }

    #[tokio::test]
    async fn test_producer_consumer_empty() {
        let result = producer_consumer(vec![]).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_fan_in() {
        let result = fan_in(3).await;
        assert_eq!(
            result,
            vec![
                "producer 0: message",
                "producer 1: message",
                "producer 2: message",
            ]
        );
    }

    #[tokio::test]
    async fn test_fan_in_single() {
        let result = fan_in(1).await;
        assert_eq!(result, vec!["producer 0: message"]);
    }
}
