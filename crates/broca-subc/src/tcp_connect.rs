//! Bounded endpoint selection shared by the consumer and module sockets.

use std::{future::Future, io, time::Duration};
use tokio::{net::TcpStream, time};

const CONNECT_BUDGET: Duration = Duration::from_secs(2);

/// Try endpoints in file order within one budget, using tuples to support raw IPv6 hosts.
pub async fn connect_endpoints<'a>(
    endpoints: impl IntoIterator<Item = (&'a str, u16)>,
) -> io::Result<TcpStream> {
    let endpoints: Vec<_> = endpoints
        .into_iter()
        .map(|(host, port)| (host.to_owned(), port))
        .collect();
    connect_with(&endpoints, TcpStream::connect).await
}

async fn connect_with<T, F, Fut>(endpoints: &[(String, u16)], mut connect: F) -> io::Result<T>
where
    F: FnMut((String, u16)) -> Fut,
    Fut: Future<Output = io::Result<T>>,
{
    let deadline = time::Instant::now() + CONNECT_BUDGET;
    let mut error = io::Error::new(
        io::ErrorKind::InvalidInput,
        "connection file has no endpoints",
    );
    for (index, endpoint) in endpoints.iter().enumerate() {
        // Reserve a share for every remaining endpoint so a blackhole cannot starve fallback.
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        let share = remaining / (endpoints.len() - index) as u32;
        match time::timeout(share, connect(endpoint.clone())).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(failed)) => error = failed,
            Err(_) => error = io::Error::new(io::ErrorKind::TimedOut, "TCP connect budget expired"),
        }
    }
    Err(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn ipv6_endpoint_connects() {
        let listener = TcpListener::bind(("::1", 0)).await.unwrap();
        let stream = connect_endpoints([("::1", listener.local_addr().unwrap().port())])
            .await
            .expect("raw IPv6 host must connect without string address formatting");
        assert_eq!(stream.peer_addr().unwrap(), listener.local_addr().unwrap());
        listener.accept().await.unwrap();
    }

    #[tokio::test]
    async fn failed_first_endpoint_tries_second() {
        let first = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let closed_port = first.local_addr().unwrap().port();
        let second = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        drop(first);
        let stream = connect_endpoints([
            ("127.0.0.1", closed_port),
            ("127.0.0.1", second.local_addr().unwrap().port()),
        ])
        .await
        .expect("a refused endpoint must not suppress later endpoints");
        assert_eq!(stream.peer_addr().unwrap(), second.local_addr().unwrap());
        second.accept().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn blackholed_endpoint_is_bounded_by_budget() {
        let endpoints = [("blackhole".into(), 1)];
        let attempted = std::sync::Arc::new(tokio::sync::Notify::new());
        let gate = std::sync::Arc::clone(&attempted);
        let connecting = connect_with::<(), _, _>(&endpoints, move |_| {
            gate.notify_one();
            std::future::pending()
        });
        tokio::pin!(connecting);
        assert!(connecting.as_mut().now_or_never().is_none());
        attempted.notified().await;
        time::advance(Duration::from_secs(2)).await;
        let error = connecting
            .now_or_never()
            .expect("connect must finish within two seconds")
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn blackholed_first_endpoint_leaves_budget_for_second() {
        let endpoints = [("blackhole".into(), 1), ("healthy".into(), 2)];
        let connecting = connect_with(&endpoints, |(_, port)| async move {
            if port == 1 {
                std::future::pending::<()>().await;
            }
            Ok(port)
        });
        tokio::pin!(connecting);
        assert!(connecting.as_mut().now_or_never().is_none());
        time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            connecting
                .now_or_never()
                .expect("fallback must receive a budget")
                .unwrap(),
            2
        );
    }
}
