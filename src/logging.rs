use std::sync::OnceLock;

static INIT: OnceLock<()> = OnceLock::new();

/// Initialize global logging (idempotent). Includes timestamp, level, file and line.
pub fn init_logging() {
    INIT.get_or_init(|| {
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .format(|buf, record| {
                use std::io::Write;
                let ts = buf.timestamp_millis();
                let file = record.file().unwrap_or("?");
                let line = record
                    .line()
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "?".into());
                writeln!(
                    buf,
                    "{ts} [{:<5}] {}:{} {}",
                    record.level(),
                    file,
                    line,
                    record.args()
                )
            })
            .try_init(); // ignore error if already initialized by tests/other code
    });
}
