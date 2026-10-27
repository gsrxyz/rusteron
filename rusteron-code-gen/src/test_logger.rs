use log::{LevelFilter, Log, Metadata, Record};
use std::io::Write;
use std::sync::Once;

struct StderrLogger;

impl Log for StderrLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            // `eprintln!` panics if stderr fails, and this can run inside an Aeron callback
            let _ = writeln!(std::io::stderr(), "{} {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

static LOGGER: StderrLogger = StderrLogger;
static INIT: Once = Once::new();

pub fn init(level: LevelFilter) {
    INIT.call_once(|| {
        let _ = log::set_logger(&LOGGER);
    });
    log::set_max_level(level);
}
