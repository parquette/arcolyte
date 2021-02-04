
use crate::errors::*;
use crate::request::Request;
use crate::response::Response;

use reqwest::Client;

/// Perform a single `GET` request.
pub fn send_request(req: &Request) -> Result<Response> {
    info!("Sending a GET request to {}", req.destination);
    if log_enabled!(::log::LogLevel::Debug) {
        debug!("Sending {} Headers", req.headers.len());
        for header in req.headers.iter() {
            debug!("\t{}: {}", header.name(), header.value_string());
        }
        for cookie in req.cookies.iter() {
            debug!("\t{} = {}", cookie.name(), cookie.value());
        }
    }

    let client = Client::builder()
        .build()
        .chain_err(|| "The native TLS backend couldn't be initialized")?;

    client
        .execute(req.to_reqwest())
        .chain_err(|| "The request failed")
        .and_then(|r| Response::from_reqwest(r))
}


