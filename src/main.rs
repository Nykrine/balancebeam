mod request;
mod response;

use clap::Parser;
use rand::{Rng, SeedableRng};
use std::net::{TcpListener, TcpStream};

/// Contains information parsed from the command-line invocation of balancebeam. 
/// The Clap macros provide a fancy way to automatically construct a command-line argument parser.
#[derive(Parser, Debug)]
#[command(about = "Fun with load balancing", name = "balancebeam")]
struct CmdOptions {
    /// IP/port to bind to
    #[arg(short, long, default_value = "0.0.0.0:1100")]
    bind: String,

    /// Upstream host to forward requests to
    #[arg(short, long)]
    upstream: Vec<String>,

    /// Perform active health checks on this interval (in seconds)
    #[arg(long, default_value_t = 10)]
    active_health_check_interval: usize,

    /// Path to send request to for active health checks
    #[arg(long, default_value = "/")]
    active_health_check_path: String,

    /// Maximum number of requests to accept per IP per minute (0 = unlimited)
    #[arg(long, default_value_t = 0)]
    max_requests_per_minute: usize,
}

/// Contains information about the state of balancebeam (e.g., what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
struct ProxyState {
    /// How frequently we check whether upstream servers are alive
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute
    max_requests_per_minute: usize,
    /// Addresses of servers that we are proxying to
    upstream_addresses: Vec<String>,
}

fn main() {
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    let options = CmdOptions::parse();
    if options.upstream.is_empty() {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    let listener = TcpListener::bind(&options.bind).unwrap_or_else(|err| {
        log::error!("Could not bind to {}: {}", options.bind, err);
        std::process::exit(1);
    });
    log::info!("Listening for requests on {}", options.bind);

    let state = ProxyState {
        upstream_addresses: options.upstream,
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
    };

    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            handle_connection(stream, &state);
        }
    }
}

fn connect_to_upstream(state: &ProxyState) -> Result<TcpStream, std::io::Error> {
    let mut rng = rand::rngs::StdRng::from_entropy();
    let upstream_idx = rng.gen_range(0..state.upstream_addresses.len());
    let upstream_ip = &state.upstream_addresses[upstream_idx];
    TcpStream::connect(upstream_ip).or_else(|err| {
        log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
        Err(err)
    })
}

fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("{} <- {}", client_ip, response::format_response_line(&response));
    if let Err(error) = response::write_to_stream(&response, client_conn) {
        log::warn!("Failed to send response to client: {}", error);
    }
}

fn handle_connection(mut client_conn: TcpStream, state: &ProxyState) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    let mut upstream_conn = match connect_to_upstream(state) {
        Ok(stream) => stream,
        Err(_) => {
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response);
            return;
        }
    };

    loop {
        let mut request = match request::read_from_stream(&mut client_conn) {
            Ok(req) => req,
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(
                    match error {
                        request::Error::IncompleteRequest(_)
                        | request::Error::MalformedRequest(_)
                        | request::Error::InvalidContentLength
                        | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                        request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                        request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                    }
                );
                send_response(&mut client_conn, &response);
                continue;
            }
        };

        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn) {
            log::error!("Failed to send request to upstream {}: {}", state.upstream_addresses[0], error);
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response);
            return;
        }

        let response = match response::read_from_stream(&mut upstream_conn, request.method()) {
            Ok(resp) => resp,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response);
                return;
            }
        };
        send_response(&mut client_conn, &response);
    }
}