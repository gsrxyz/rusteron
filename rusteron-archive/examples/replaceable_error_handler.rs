//! Replace an error callback without registering again.
//! No archive server is needed.

use rusteron_archive::{AeronArchiveContext, AeronErrorHandlerSlot};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = AeronArchiveContext::new()?;
    let handler = AeronErrorHandlerSlot::new();

    // Register once.
    ctx.set_error_handler(Some(handler.clone()))?;

    let label = String::from("first");
    handler.set(move |code, message| eprintln!("{label}: {code}: {message}"));
    ctx.invoke_error_handler(1, 0, c"test");

    // The old callback drops its label after its last call.
    handler.set(|code, message| eprintln!("second: {code}: {message}"));
    ctx.invoke_error_handler(2, 0, c"test");

    handler.clear();
    ctx.invoke_error_handler(3, 0, c"ignored");
    Ok(())
}
