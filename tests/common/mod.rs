mod balancebeam;
mod echo_server;
mod error_server;
mod server;

use std::sync;

pub use balancebeam::BalanceBeam;
pub use echo_server::EchoServer;
pub use error_server::ErrorServer;
pub use server::Server;

static INIT_TESTS: sync::Once = sync::Once::new();

/// Initializes logging for tests.
///
/// This function initializes the logging system for running tests. It uses the `pretty_env_logger`
/// crate to configure the logger with the following settings:
///
/// - `is_test(true)`: Indicates that the logger is being used in a test environment.
/// - `parse_filters("info")`: Sets the log level to `info`, which means only log messages with a
///   severity level of `info` or higher will be displayed.
///
/// This function is designed to be called once at the beginning of the test suite, ensuring that
/// the logging system is properly configured before any tests are run.
pub fn init_logging() {
    INIT_TESTS.call_once(|| {
        pretty_env_logger::formatted_builder()
            .is_test(true)
            .parse_filters("info")
            .init();
    });
}