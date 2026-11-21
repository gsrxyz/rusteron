//! Count status messages and delegate sender limits to Aeron's maximum flow-control policy.
//! Run with `cargo run -p rusteron-media-driver --features static --example flow_control`.
//! Connect clients to the printed directory. Press Enter to stop.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

use std::sync::atomic::{AtomicU64, Ordering};
pub static FLOW_MESSAGES: AtomicU64 = AtomicU64::new(0);

/// Add observation to a complete flow-control policy by delegation.
struct ObservedFlow(MaxFlowControl);
impl FlowControl for ObservedFlow {
    type Shared = ();
    fn new(args: &FlowControlArgs<'_>) -> Result<(Self, ()), AeronCError> {
        Ok((Self(MaxFlowControl::new(args)?.0), ()))
    }
    fn on_status_message(
        &mut self,
        shared: &(),
        message: StatusMessage<'_>,
        source: SockAddrRef<'_>,
        limit: i64,
        term: i32,
        shift: usize,
        now: i64,
        metrics: StrategyMetrics<'_>,
    ) -> Position {
        FLOW_MESSAGES.fetch_add(1, Ordering::Relaxed);
        self.0
            .on_status_message(shared, message, source, limit, term, shift, now, metrics)
    }
}

/// Install this extension before creating the driver.
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
