//! `muse-acp` binary: ACP agent over stdio, plus the `auth status` probe.
//!
//! With no arguments, the binary speaks ACP on stdin/stdout (see
//! [`muse_acp::acp`]) and logs to stderr — stdout is the protocol channel
//! and must carry nothing else.
//!
//! `muse-acp auth status` is the desktop catalog's auth probe: it exits 0
//! when a previous Muse sign-in was recorded in the bridge profile, and 1
//! otherwise, without launching a browser. When `MUSE_ACP_CDP_ENDPOINT` is
//! set, it instead verifies the live Muse composer through the attached
//! operator-owned browser.

use std::sync::Arc;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use muse_acp::acp::run_agent;
use muse_acp::auth;
use muse_acp::chrome;
use muse_acp::muse_web::MuseWebDriver;
use muse_acp::selectors;
use serde_json::Value;
use std::time::{Duration, Instant};

const USAGE: &str = "usage: muse-acp [auth status | cdp status]";

const CDP_STATUS_WAIT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct CdpStatus {
    endpoint: String,
    websocket_connected: bool,
    muse_tab_ready: Option<bool>,
    detail: String,
}

async fn cdp_status(endpoint: &str) -> Result<CdpStatus, String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err("MUSE_ACP_CDP_ENDPOINT is set but empty".into());
    }
    let started = Instant::now();
    let browser = chrome::attach(endpoint)
        .await
        .map_err(|error| error.to_string())?;
    let tab = browser
        .new_tab(selectors::MUSE_CHAT_URL)
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let probe = tab
            .evaluate_json(selectors::PROBE_JS)
            .await
            .map_err(|error| error.to_string())?;
        if probe.get("composer").and_then(Value::as_str).is_some() {
            let _ = tab.close().await;
            return Ok(CdpStatus {
                endpoint: endpoint.to_string(),
                websocket_connected: true,
                muse_tab_ready: Some(true),
                detail:
                    "Muse composer detected; the attached browser is authenticated for the bridge"
                        .into(),
            });
        }
        if probe
            .get("loginWall")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let _ = tab.close().await;
            return Ok(CdpStatus {
                endpoint: endpoint.to_string(),
                websocket_connected: true,
                muse_tab_ready: Some(false),
                detail: "CDP is reachable, but Muse shows a login wall".into(),
            });
        }
        if started.elapsed() >= CDP_STATUS_WAIT {
            let _ = tab.close().await;
            return Ok(CdpStatus {
                endpoint: endpoint.to_string(),
                websocket_connected: true,
                muse_tab_ready: None,
                detail: "CDP is reachable, but Muse did not expose a composer within 10 seconds"
                    .into(),
            });
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn run_cdp_status(endpoint: &str) {
    match cdp_status(endpoint).await {
        Ok(status) => {
            println!(
                "{}",
                serde_json::json!({
                    "endpoint": status.endpoint,
                    "websocket_connected": status.websocket_connected,
                    "muse_tab_ready": status.muse_tab_ready,
                    "detail": status.detail,
                })
            );
            if status.muse_tab_ready != Some(true) {
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("cdp status failed: {error}");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(std::env::var("MUSE_ACP_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args
        .iter()
        .map(|arg| arg.as_str())
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => run_stdio().await,
        ["auth", "status"] => {
            if let Ok(endpoint) = std::env::var("MUSE_ACP_CDP_ENDPOINT") {
                // For an operator-owned browser, the bridge marker is not
                // authoritative: prove the live composer over CDP instead.
                run_cdp_status(&endpoint).await;
            } else if auth::has_saved_session() {
                println!("signed in to Muse (saved bridge session present)");
                std::process::exit(0);
            } else {
                println!("no saved Muse session");
                std::process::exit(1);
            }
        }
        ["cdp", "status"] => {
            let Some(endpoint) = std::env::var("MUSE_ACP_CDP_ENDPOINT").ok() else {
                eprintln!("cdp status requires MUSE_ACP_CDP_ENDPOINT");
                std::process::exit(2);
            };
            run_cdp_status(&endpoint).await;
        }
        ["--help" | "-h"] => {
            println!("{USAGE}");
            println!();
            println!("Speaks ACP over stdio when run with no arguments.");
            println!("auth status: exit 0 when a saved Muse session exists, 1 otherwise.");
            println!(
                "cdp status: attach to MUSE_ACP_CDP_ENDPOINT and verify the live Muse composer."
            );
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
}

/// Wire stdio to the agent loop: stdin lines in, outbound frames to
/// stdout, one frame per line.
async fn run_stdio() {
    let (frames_tx, mut frames_rx) = mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(frame) = frames_rx.recv().await {
            if stdout.write_all(frame.as_bytes()).await.is_err() {
                break;
            }
            if stdout.write_all(b"\n").await.is_err() {
                break;
            }
            if stdout.flush().await.is_err() {
                break;
            }
        }
    });
    let driver = Arc::new(MuseWebDriver::new());
    run_agent(BufReader::new(tokio::io::stdin()), frames_tx, driver).await;
    // Input ended: the harness is gone. Stop forwarding and exit rather
    // than waiting on turn tasks that were just cancelled.
    writer.abort();
}
