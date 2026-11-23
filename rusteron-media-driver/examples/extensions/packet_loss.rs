//! Drop every seventh outgoing batch after a fixed seeded offset. Use only for retransmission tests.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};

/// Repeatable packet loss for tests. Every Nth batch is dropped after a seeded
/// offset. Each agent has its own counter. N=0 disables loss.
pub struct SeededLoss<const N: u64, const SEED: u64 = 1> {
    count: u64,
}
impl<const N: u64, const SEED: u64> Default for SeededLoss<N, SEED> {
    fn default() -> Self {
        Self { count: SEED }
    }
}
impl<const N: u64, const SEED: u64> InterceptorStateFactory for SeededLoss<N, SEED> {
    type State = ();
    fn new(_: &(), _: StrategyContext, _: TransportAffinity) -> Result<Self, AeronCError> {
        Ok(Self::default())
    }
}
impl<const N: u64, const SEED: u64> OutgoingInterceptor for SeededLoss<N, SEED> {
    #[inline]
    fn send<'a>(&mut self, next: OutgoingNext<'a>, packet: OutgoingPacket<'a>) -> i32 {
        self.count = self.count.wrapping_add(1);
        if N != 0 && self.count.is_multiple_of(N) {
            packet.drop_as_sent()
        } else {
            next.send(packet)
        }
    }
}

pub fn configure(context: &AeronDriverContext) -> Result<(), AeronCError> {
    context.add_outgoing_interceptor::<SeededLoss<7, 3>>()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = rusteron_media_driver::testing::EmbeddedDriver::launch_with(configure)?;
    println!("Driver directory: {}", driver.dir());
    println!("Use aeron:udp?endpoint=127.0.0.1:40123. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    drop(driver);
    Ok(())
}
