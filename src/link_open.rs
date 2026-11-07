//! Local terminal-link handoff. Private, ephemeral endpoints belong to this plugin,
//! scoped to the exact Herdr session and tab; no Herdr wire changes are needed.
use crate::{
    controller::Controller,
    herdr::{HerdrCli, LiveHerdr},
    open_target::OpenTarget,
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Serialize, Deserialize)]
struct Endpoint {
    session: String,
    tab: String,
    pane: String,
    address: String,
    token: String,
    used: u128,
}
#[derive(Serialize, Deserialize)]
struct Request {
    token: String,
    target: OpenTarget,
}

fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
fn directory() -> Option<PathBuf> {
    Some(PathBuf::from(std::env::var_os("HERDR_PLUGIN_STATE_DIR")?).join("link-endpoints"))
}
fn context() -> serde_json::Value {
    std::env::var("HERDR_PLUGIN_CONTEXT_JSON")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}
fn text(value: &serde_json::Value, field: &str) -> io::Result<String> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other(format!("missing {field}")))
}

pub struct Receiver {
    endpoint: Endpoint,
    path: PathBuf,
    requests: mpsc::Receiver<OpenTarget>,
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl Receiver {
    pub fn bind() -> io::Result<Option<Self>> {
        let Some(dir) = directory() else {
            return Ok(None);
        };
        let ctx = context();
        let Ok(session) = std::env::var("HERDR_SOCKET_PATH") else {
            return Ok(None);
        };
        let Ok(tab) = text(&ctx, "tab_id") else {
            return Ok(None);
        };
        let pane = std::env::var("HERDR_PANE_ID").map_err(io::Error::other)?;
        Self::bind_in(&dir, session, tab, pane).map(Some)
    }
    fn bind_in(dir: &Path, session: String, tab: String, pane: String) -> io::Result<Self> {
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        let mut random = [0u8; 24];
        fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        let token: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = Endpoint {
            session,
            tab,
            pane,
            address: listener.local_addr()?.to_string(),
            token,
            used: now(),
        };
        let path = dir.join(format!("{}.json", endpoint.token));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        serde_json::to_writer(&mut file, &endpoint)?;
        let (tx, requests) = mpsc::sync_channel(16);
        let secret = endpoint.token.clone();
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_stopped = stopped.clone();
        std::thread::spawn(move || {
            for incoming in listener.incoming() {
                let Ok(mut stream) = incoming else { break };
                if worker_stopped.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
                let mut bytes = Vec::new();
                let read = (&mut stream).take(16 * 1024 + 1).read_to_end(&mut bytes);
                let accepted = read.is_ok()
                    && bytes.len() <= 16 * 1024
                    && serde_json::from_slice::<Request>(&bytes)
                        .ok()
                        .is_some_and(|req| req.token == secret && tx.try_send(req.target).is_ok());
                let _ = stream.write_all(if accepted { b"OK" } else { b"NO" });
            }
        });
        Ok(Self {
            endpoint,
            path,
            requests,
            stopped,
        })
    }
    pub fn focused(&mut self) {
        self.endpoint.used = now();
        use std::os::unix::fs::OpenOptionsExt;
        let temporary = self.path.with_extension("tmp");
        if let Ok(mut file) = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
        {
            if serde_json::to_writer(&mut file, &self.endpoint).is_ok() {
                let _ = fs::rename(&temporary, &self.path);
            }
            let _ = fs::remove_file(temporary);
        }
    }
    pub fn poll(&self, controller: &mut Controller) -> bool {
        let mut dirty = false;
        while let Ok(target) = self.requests.try_recv() {
            controller.open_link_target(&target);
            dirty = true;
        }
        dirty
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
        // Wake the blocking accept so the listener thread releases its port on exit.
        if let Ok(address) = self.endpoint.address.parse() {
            let _ = TcpStream::connect_timeout(&address, Duration::from_millis(50));
        }
        let _ = fs::remove_file(&self.path);
    }
}

/// Resolve against the clicked pane's cwd, never the plugin process's cwd.
pub fn resolve(raw: &str, cwd: &Path) -> io::Result<OpenTarget> {
    let uri = raw.starts_with("file:");
    let raw = raw.strip_prefix("file:").unwrap_or(raw);
    let raw = if let Some(rest) = raw.strip_prefix("//") {
        if rest.starts_with('/') {
            rest
        } else if let Some(rest) = rest.strip_prefix("localhost/") {
            // Keep the leading slash below.
            return resolve(&format!("file:///{}", rest), cwd);
        } else {
            return Err(io::Error::other("non-local file URI"));
        }
    } else {
        raw
    };
    let decoded;
    let raw = if uri {
        let mut bytes = Vec::new();
        let mut chars = raw.bytes();
        while let Some(b) = chars.next() {
            if b == b'%' {
                let a = chars.next().and_then(|c| (c as char).to_digit(16));
                let b = chars.next().and_then(|c| (c as char).to_digit(16));
                let (Some(a), Some(b)) = (a, b) else {
                    return Err(io::Error::other("invalid file URI escape"));
                };
                bytes.push((a * 16 + b) as u8);
            } else {
                bytes.push(b);
            }
        }
        decoded = String::from_utf8(bytes).map_err(io::Error::other)?;
        decoded.as_str()
    } else {
        raw
    };
    if raw.chars().any(char::is_control) {
        return Err(io::Error::other("invalid file path"));
    }
    let expanded = if let Some(rest) = raw.strip_prefix("~/") {
        PathBuf::from(std::env::var_os("HOME").ok_or_else(|| io::Error::other("HOME unavailable"))?)
            .join(rest)
    } else if let Some(rest) = raw.strip_prefix("$PWD/") {
        cwd.join(rest)
    } else {
        cwd.join(raw)
    };
    // A literal filename ending in :digits wins over source-location syntax.
    let mut path = expanded.to_string_lossy().into_owned();
    let mut numbers = Vec::new();
    if !expanded.exists() {
        for _ in 0..2 {
            let Some((head, tail)) = path.rsplit_once(':') else {
                break;
            };
            if tail.is_empty() || !tail.bytes().all(|b| b.is_ascii_digit()) {
                break;
            }
            let n = tail.parse::<usize>().map_err(io::Error::other)?;
            numbers.push(n.max(1));
            path = head.to_owned();
        }
    }
    let line = numbers.pop();
    // Preserve :line:column recognition; the viewer currently navigates by source line.
    let path = fs::canonicalize(&path)?;
    if !path.is_file() {
        return Err(io::Error::other("link target is not a file"));
    }
    Ok(OpenTarget {
        path: path.to_string_lossy().into_owned(),
        line,
        end_line: None,
    })
}

pub fn invoke() -> io::Result<()> {
    invoke_with(&LiveHerdr::from_env())
}
fn invoke_with(cli: &impl HerdrCli) -> io::Result<()> {
    let ctx = context();
    let session = std::env::var("HERDR_SOCKET_PATH").map_err(io::Error::other)?;
    let (config, _) = crate::config::load_config_from_env();
    let direction = crate::config::resolve(&config, |key| std::env::var(key).ok()).open_direction;
    invoke_context(
        cli,
        &ctx,
        &session,
        directory().as_deref(),
        direction.label(),
    )
}
fn invoke_context(
    cli: &impl HerdrCli,
    ctx: &serde_json::Value,
    session: &str,
    directory: Option<&Path>,
    direction: &str,
) -> io::Result<()> {
    let cwd = text(ctx, "focused_pane_cwd").or_else(|_| text(ctx, "workspace_cwd"))?;
    let target = resolve(&text(ctx, "clicked_url")?, Path::new(&cwd))?;
    let tab = text(ctx, "tab_id")?;
    let pane = text(ctx, "focused_pane_id")?;
    let panes: serde_json::Value = serde_json::from_str(&cli.run_json(&["pane", "list"])?)?;
    let live = panes["result"]["panes"]
        .as_array()
        .ok_or_else(|| io::Error::other("missing pane list"))?;
    if !live
        .iter()
        .any(|p| p["pane_id"] == pane && p["tab_id"] == tab)
    {
        return Err(io::Error::other("source pane moved before file navigation"));
    }
    let mut endpoints = Vec::new();
    if let Some(dir) = directory
        && let Ok(entries) = fs::read_dir(dir)
    {
        for entry in entries.flatten().take(1024) {
            if let Ok(bytes) = fs::read(entry.path())
                && let Ok(endpoint) = serde_json::from_slice::<Endpoint>(&bytes)
                && endpoint.session == session
                && live
                    .iter()
                    .any(|p| p["pane_id"] == endpoint.pane && p["tab_id"] == tab)
            {
                endpoints.push((entry.path(), endpoint));
            }
        }
    }
    endpoints.sort_by_key(|(_, ep)| std::cmp::Reverse(ep.used));
    for (path, ep) in endpoints {
        // Metadata is local, but never connect to a non-loopback address.
        let Ok(addr) = ep.address.parse::<std::net::SocketAddr>() else {
            continue;
        };
        if !addr.ip().is_loopback() {
            continue;
        }
        let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) else {
            let _ = fs::remove_file(path);
            continue;
        };
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        stream.set_write_timeout(Some(Duration::from_secs(1)))?;
        serde_json::to_writer(
            &mut stream,
            &Request {
                token: ep.token,
                target: target.clone(),
            },
        )?;
        stream.shutdown(std::net::Shutdown::Write)?;
        let mut reply = [0u8; 2];
        if stream.read_exact(&mut reply).is_ok() && &reply == b"OK" {
            return Ok(());
        }
    }
    let parent = Path::new(&target.path)
        .parent()
        .ok_or_else(|| io::Error::other("missing parent"))?;
    let root = crate::root::resolve(&crate::context::LaunchContext {
        cwd: parent.to_path_buf(),
        ..Default::default()
    })
    .root;
    let open = format!("HERDR_FILE_VIEWER_OPEN={}", target.display_ref());
    let root = format!("HERDR_FILE_VIEWER_ROOT={}", root.display());
    // Verified with Herdr 0.9.5 `plugin pane open --help`: target-pane, --no-focus and --env.
    cli.run(&[
        "plugin",
        "pane",
        "open",
        "--plugin",
        "herdr-file-viewer",
        "--entrypoint",
        "file-viewer",
        "--target-pane",
        &pane,
        "--direction",
        direction,
        "--no-focus",
        "--env",
        &open,
        "--env",
        &root,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let dir =
                std::env::temp_dir().join(format!("hfv-links-{}-{}", std::process::id(), now()));
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    struct Host {
        panes: serde_json::Value,
        calls: RefCell<Vec<Vec<String>>>,
    }
    impl HerdrCli for Host {
        fn run_json(&self, args: &[&str]) -> io::Result<String> {
            self.calls
                .borrow_mut()
                .push(args.iter().map(|s| s.to_string()).collect());
            Ok(self.panes.to_string())
        }
    }
    #[test]
    fn file_link_resolution_uses_source_cwd_and_rejects_invalid_uris() {
        let dir = Temp::new();
        let path = dir.0.join("你好 file.rs");
        fs::write(&path, "source").unwrap();
        let expected = path.canonicalize().unwrap().to_string_lossy().into_owned();
        for raw in [
            "file:你好 file.rs:42:7".to_owned(),
            format!("file://{}:42:7", path.display()).replace(' ', "%20"),
        ] {
            let target = resolve(&raw, &dir.0).unwrap();
            assert_eq!(target.path, expected);
            assert_eq!(target.line, Some(42));
        }
        for raw in [
            "file://remote/tmp/a.rs",
            "file:///tmp/%GG",
            "file:missing.rs",
            "file:bad\0.rs",
        ] {
            assert!(resolve(raw, &dir.0).is_err(), "{raw}");
        }
        let literal = dir.0.join("notes:12");
        fs::write(&literal, "literal").unwrap();
        assert_eq!(resolve("file:notes:12", &dir.0).unwrap().line, None);
    }
    #[test]
    fn repeated_links_reuse_most_recent_same_tab_viewer_without_layout_commands() {
        let dir = Temp::new();
        let target = dir.0.join("test.rs");
        fs::write(&target, "test").unwrap();
        let mut first =
            Receiver::bind_in(&dir.0, "session".into(), "tab".into(), "one".into()).unwrap();
        let second =
            Receiver::bind_in(&dir.0, "session".into(), "tab".into(), "two".into()).unwrap();
        let other_session =
            Receiver::bind_in(&dir.0, "different".into(), "tab".into(), "two".into()).unwrap();
        let other_tab =
            Receiver::bind_in(&dir.0, "session".into(), "elsewhere".into(), "three".into())
                .unwrap();
        first.focused();
        let host = Host {
            panes: serde_json::json!({"result":{"panes":[
                {"pane_id":"source","tab_id":"tab"}, {"pane_id":"one","tab_id":"tab"},
                {"pane_id":"two","tab_id":"tab"}, {"pane_id":"three","tab_id":"elsewhere"}
            ]}}),
            calls: RefCell::new(Vec::new()),
        };
        let ctx = serde_json::json!({"clicked_url":"file:test.rs:5", "focused_pane_cwd":dir.0,
            "tab_id":"tab", "focused_pane_id":"source"});
        for _ in 0..2 {
            invoke_context(&host, &ctx, "session", Some(&dir.0), "right").unwrap();
            assert_eq!(
                first
                    .requests
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap()
                    .line,
                Some(5)
            );
        }
        assert!(second.requests.try_recv().is_err());
        assert!(other_session.requests.try_recv().is_err());
        assert!(other_tab.requests.try_recv().is_err());
        assert_eq!(host.calls.borrow().len(), 2);
        assert!(
            host.calls
                .borrow()
                .iter()
                .all(|args| args == &["pane", "list"])
        );
    }
    #[test]
    fn new_viewer_is_targeted_without_focus_and_stale_source_is_rejected() {
        let dir = Temp::new();
        fs::write(dir.0.join("test.rs"), "test").unwrap();
        let host = Host {
            panes: serde_json::json!({"result":{"panes":[{"pane_id":"source","tab_id":"tab"}]}}),
            calls: RefCell::new(Vec::new()),
        };
        let mut ctx = serde_json::json!({"clicked_url":"file:test.rs:5", "workspace_cwd":dir.0,
            "tab_id":"tab", "focused_pane_id":"source"});
        invoke_context(&host, &ctx, "session", Some(&dir.0), "right").unwrap();
        let calls = host.calls.borrow();
        let args = &calls[1];
        assert!(args.contains(&"--no-focus".to_owned()));
        assert!(args.windows(2).any(|p| p == ["--target-pane", "source"]));
        assert!(
            args.iter()
                .any(|arg| arg.starts_with("HERDR_FILE_VIEWER_OPEN="))
        );
        drop(calls);
        ctx["tab_id"] = "moved".into();
        assert!(invoke_context(&host, &ctx, "session", Some(&dir.0), "right").is_err());
        assert_eq!(host.calls.borrow().len(), 3);
    }
    #[test]
    fn invalid_token_never_reaches_viewer() {
        let dir = Temp::new();
        let receiver =
            Receiver::bind_in(&dir.0, "session".into(), "tab".into(), "pane".into()).unwrap();
        let mut stream = TcpStream::connect(&receiver.endpoint.address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        serde_json::to_writer(
            &mut stream,
            &Request {
                token: "wrong".into(),
                target: OpenTarget {
                    path: "/tmp/file".into(),
                    line: None,
                    end_line: None,
                },
            },
        )
        .unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let mut reply = [0u8; 2];
        stream.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"NO");
        assert!(receiver.requests.try_recv().is_err());
    }
}
