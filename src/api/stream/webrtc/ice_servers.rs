use crate::{
    api::bindings::RtcIceServer,
    config::{TurnRestConfig, WebRtcConfig},
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use hmac::{Hmac, Mac};
use log::error;
use sha1::Sha1;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tracing::debug;

use crate::app::{App, AppError};

pub async fn generate_ice_servers(app: &App) -> Result<Vec<RtcIceServer>, AppError> {
    // Load ice servers
    let mut ice_servers = app.config().webrtc.ice_servers.clone();

    // Load dynamic ice servers and append them to the current ice servers
    let dynamic_ice_servers = load_dynamic_ice_servers(&app.config().webrtc).await;
    ice_servers.extend_from_slice(&dynamic_ice_servers);

    // Mint a fresh time-limited TURN credential for this negotiation.
    if let Some(turn_rest) = app.config().webrtc.turn_rest.as_ref()
        && let Some(server) = generate_turn_rest_server(turn_rest)
    {
        ice_servers.push(server);
    }

    Ok(ice_servers)
}

async fn load_dynamic_ice_servers(config: &WebRtcConfig) -> Vec<RtcIceServer> {
    let Some(script_command) = config.ice_server_script.as_ref() else {
        debug!("No WebRTC ice server script found");
        return vec![];
    };

    debug!(script = script_command, "running WebRTC ice server script");

    let mut script = Command::new(script_command);

    let output = match script.output().await {
        Ok(value) => value,
        Err(err) => {
            error!("failed to run WebRTC ice server script: {err}");
            return vec![];
        }
    };

    if !matches!(output.status.code(), None | Some(0)) {
        error!(
            "WebRTC ice server script has a non zero exit code: {}",
            output.status
        );

        if let Ok(error) = String::from_utf8(output.stdout) {
            error!("WebRTC ice server script error:\n{error}");
        }
        return vec![];
    }

    let json: Vec<RtcIceServer> = match serde_json::from_slice(&output.stdout) {
        Ok(value) => value,
        Err(err) => {
            error!("failed to deserialize WebRTC ice server script output: {err}");
            return vec![];
        }
    };

    json
}

/// Builds a TURN credential for a coturn server in `use-auth-secret` mode.
///
/// coturn accepts `username = <unix-expiry>:<name>` with
/// `password = base64(HMAC-SHA1(username, static-auth-secret))`. Because this
/// runs per negotiation the credential is short-lived, so unlike a static
/// username/password in the config it cannot silently expire.
///
/// Returns None rather than failing the stream: TURN is a fallback path, and a
/// misconfigured secret should not stop a direct or STUN connection working.
fn generate_turn_rest_server(config: &TurnRestConfig) -> Option<RtcIceServer> {
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(value) => value.as_secs(),
        Err(err) => {
            error!("system clock is before the unix epoch, cannot build turn credential: {err}");
            return None;
        }
    };

    let username = format!(
        "{}:{}",
        now.saturating_add(config.ttl_seconds),
        config.username
    );

    let mut mac = match Hmac::<Sha1>::new_from_slice(config.secret.as_bytes()) {
        Ok(value) => value,
        Err(err) => {
            error!("invalid webrtc.turn_rest.secret: {err}");
            return None;
        }
    };
    mac.update(username.as_bytes());

    let credential = BASE64_STANDARD.encode(mac.finalize().into_bytes());
    debug!(username = username, "generated turn rest credential");

    Some(RtcIceServer {
        is_default: false,
        urls: config.urls.clone(),
        username,
        credential,
    })
}
