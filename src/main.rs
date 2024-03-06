mod request;
mod response;

use clap::Parser;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::time;
use tokio::net::TcpListener;
use tokio::{net::TcpStream, sync::RwLock};

/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Parser, Debug)]
#[clap(about = "Fun with load balancing")]
struct CmdOptions {
    #[clap(
        short,
        long,
        help = "IP/port to bind to",
        default_value = "0.0.0.0:1100"
    )]
    bind: String,
    #[clap(short, long, help = "Upstream host to forward requests to")]
    upstream: Vec<String>,
    #[clap(
        long,
        help = "Perform active health checks on this interval (in seconds)",
        default_value = "10"
    )]
    active_health_check_interval: usize,
    #[clap(
        long,
        help = "Path to send request to for active health checks",
        default_value = "/"
    )]
    active_health_check_path: String,
    #[clap(
        long,
        help = "Maximum number of requests to accept per IP per minute (0 = unlimited)",
        default_value = "0"
    )]
    max_requests_per_minute: usize,
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct in later milestones.
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    max_requests_per_minute: usize,
    /// Addresses of servers that we are proxying to
    upstream_addresses: Vec<String>,
    /// The status of the upstream servers, true for up, false for down
    upstream_address_status: (Vec<bool>, usize),
    /// The number of requests we've received since the last time we checked the upstream servers
    upstream_address_request_counters: HashMap<String, usize>,
    last_rate_limiting_check_time: time::Instant,
}

#[tokio::main]
async fn main() {
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    // Start listening for connections
    let mut listener = match TcpListener::bind(&options.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    let upstream_len = options.upstream.len();

    // Handle incoming connections
    let state = Arc::new(RwLock::new(ProxyState {
        upstream_addresses: options.upstream,
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
        upstream_address_status: (vec![true; upstream_len], upstream_len),
        upstream_address_request_counters: HashMap::new(),
        last_rate_limiting_check_time: time::Instant::now(),
    }));

    let state_ref = state.clone();
    tokio::spawn(async move {
        active_health_check(state_ref).await;
    });

    while let Ok((mut stream, _addr)) = listener.accept().await {
        match stream.peer_addr() {
            Ok(peer_addr) => {
                // Check if rate limiting applies
                if state.read().await.max_requests_per_minute > 0 {
                    // Apply rate limiting
                    match rate_limiting_fixed_window(&state, &peer_addr.ip()).await {
                        Ok(_) => {
                            // Continue with handling the connection if rate limit is not exceeded
                            let state_ref = state.clone();
                            tokio::spawn(async move {
                                handle_connection(stream, state_ref).await;
                            });
                        },
                        Err(_) => {
                            // Rate limit exceeded, send error response
                            let response = response::make_http_error(http::StatusCode::TOO_MANY_REQUESTS);
                            if let Err(err) = response::write_to_stream(&response, &mut stream).await {
                                log::error!("Error writing response: {}", err);
                            }
                        }
                    }
                } else {
                    // No rate limiting, handle connection
                    let state_ref = state.clone();
                    tokio::spawn(async move {
                        handle_connection(stream, state_ref).await;
                    });
                }
            },
            Err(e) => {
                // Error getting peer address, log and continue to the next connection
                log::error!("Failed to get peer address for incoming connection: {}", e);
            }
        }
    }
}

async fn connect_to_upstream(state: Arc<RwLock<ProxyState>>) -> Result<TcpStream, std::io::Error> {
    let mut rng = rand::rngs::StdRng::from_entropy();
    loop {
        let s = state.read().await;
        let upstream_idx = rng.gen_range(0..s.upstream_addresses.len());
        let upstream_ip = &s.upstream_addresses[upstream_idx];

        if s.upstream_address_status.1 == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No valid upstream address",
            ));
        }
        if !s.upstream_address_status.0[upstream_idx] {
            continue;
        }

        match TcpStream::connect(upstream_ip).await.or_else(|err| {
            log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
            Err(err)
        }) {
            Ok(stream) => {
                return Ok(stream);
            }
            Err(_) => {
                drop(s);
                state.write().await.upstream_address_status.0[upstream_idx] = false;
                state.write().await.upstream_address_status.1 -= 1;
            }
        }
    }
}

async fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn).await {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

async fn handle_connection(mut client_conn: TcpStream, state: Arc<RwLock<ProxyState>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random destination server
    let mut upstream_conn = match connect_to_upstream(state).await {
        Ok(stream) => stream,
        Err(_error) => {
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
    };
    let upstream_ip = client_conn.peer_addr().unwrap().ip().to_string();

    // The client may now send us one or more requests. Keep trying to read requests until the
    // client hangs up or we get an error.
    loop {
        // Read a request from the client
        let mut request = match request::read_from_stream(&mut client_conn).await {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response).await;
                continue;
            }
        };
        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn).await {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(&mut upstream_conn, request.method()).await
        {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response).await;
                return;
            }
        };
        // Forward the response to the client
        send_response(&mut client_conn, &response).await;
        log::debug!("Forwarded response to client");
    }
}

async fn active_health_check(state: Arc<RwLock<ProxyState>>) {
    let s = state.read().await;
    log::debug!("{}", s.active_health_check_interval);

    let mut interval = time::interval(time::Duration::from_secs(
        s.active_health_check_interval as u64,
    ));
    let len = s.upstream_addresses.len();
    drop(s);

    // The first interval ticks immediately
    interval.tick().await;

    loop {
        interval.tick().await;
        for upstream_idx in 0..len {
            let s = state.read().await;
            let upstream_ip = &s.upstream_addresses[upstream_idx];
            let request = http::Request::builder()
                .method(http::Method::GET)
                .uri(&s.active_health_check_path)
                .header("Host", upstream_ip)
                .body(Vec::new())
                .unwrap();

            let mut upstream_conn = if let Ok(stream) = TcpStream::connect(upstream_ip).await {
                stream
            } else {
                drop(s);
                {
                    let s = state.read().await;
                    if !s.upstream_address_status.0[upstream_idx] {
                        continue;
                    }
                }
                {
                    let mut s = state.write().await;
                    s.upstream_address_status.0[upstream_idx] = false;
                    s.upstream_address_status.1 -= 1;
                }
                continue;
            };
            drop(s);

            if let Err(_) = request::write_to_stream(&request, &mut upstream_conn).await {
                log::error!("write to stream failed");
                continue;
            }

            match response::read_from_stream(&mut upstream_conn, request.method()).await {
                Ok(response) => {
                    if response.status() == http::StatusCode::OK {
                        {
                            if state.read().await.upstream_address_status.0[upstream_idx] {
                                continue;
                            }
                        }
                        {
                            let mut s = state.write().await;
                            s.upstream_address_status.0[upstream_idx] = true;
                            s.upstream_address_status.1 += 1;
                        }
                        {
                            log::debug!(
                                "Active check server {} ok, valid_num: {}",
                                upstream_idx,
                                state.read().await.upstream_address_status.1
                            );
                        }
                    } else {
                        log::debug!(
                            "status_code: {}, {}",
                            response.status().as_u16(),
                            upstream_idx
                        );
                        {
                            if !state.read().await.upstream_address_status.0[upstream_idx] {
                                continue;
                            }
                        }
                        {
                            let mut s = state.write().await;
                            s.upstream_address_status.0[upstream_idx] = false;
                            s.upstream_address_status.1 -= 1;
                        }
                        {
                            log::debug!(
                                "Active check server {} failed, valid_num: {}",
                                upstream_idx,
                                state.read().await.upstream_address_status.1
                            );
                        }
                    }
                }
                Err(_) => {
                    log::error!(
                        "Active health check upstream server {} is failed",
                        upstream_idx
                    );
                    {
                        {
                            let s = state.read().await;
                            if !s.upstream_address_status.0[upstream_idx] {
                                continue;
                            }
                        }
                        {
                            let mut s = state.write().await;
                            s.upstream_address_status.0[upstream_idx] = false;
                            s.upstream_address_status.1 -= 1;
                        }
                    }
                }
            };
        }
    }
}

async fn rate_limiting_fixed_window(
    state: &Arc<RwLock<ProxyState>>,
    client_ip: &IpAddr,
) -> Result<(), std::io::Error> {
    let max_requests = state.read().await.max_requests_per_minute;
    {
        let s = state.read().await;
        if s.last_rate_limiting_check_time.elapsed() >= time::Duration::from_secs(60) {
            // clear all requests counter
            drop(s);
            {
                let mut s = state.write().await;
                for (_, value) in s.upstream_address_request_counters.iter_mut() {
                    *value = 0;
                }
                s.last_rate_limiting_check_time = time::Instant::now();
            }
        }
    }
    {
        let mut s = state.write().await;
        let times = s
            .upstream_address_request_counters
            .entry(client_ip.to_string())
            .or_insert(0);
        *times += 1;
        if *times > max_requests {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Too many requests",
            ))
        } else {
            Ok(())
        }
    }
}