//! Count status messages and advance the sender to the furthest receiver, as Aeron's max policy does.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::sync::atomic::{AtomicU64, Ordering};
pub static FLOW_MESSAGES: AtomicU64 = AtomicU64::new(0);

/// Aeron's max policy with a status-message count.
struct ObservedFlow;
impl FlowControl for ObservedFlow {
    type Shared = ();
    fn new(_: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        Ok((Self, ()))
    }
    fn on_status_message(
        &mut self,
        _: &(),
        message: StatusMessage<'_>,
        _: SockAddrRef<'_>,
        limit: i64,
        term: i32,
        shift: usize,
        _: i64,
        _: StrategyMetrics<'_>,
    ) -> Position {
        FLOW_MESSAGES.fetch_add(1, Ordering::Relaxed);
        // The maximum receiver limit, as in Aeron's max policy
        Position::new(
            limit.max(
                message
                    .position(term, shift)
                    .saturating_add(i64::from(message.receiver_window())),
            ),
        )
    }
}

pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.set_unicast_flow_control::<ObservedFlow>()?;
    context.set_multicast_flow_control::<ObservedFlow>()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    println!("Status messages: {}", FLOW_MESSAGES.load(Ordering::Relaxed));
    Ok(())
}
