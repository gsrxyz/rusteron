//! Apply seeded burst loss to one UDP destination. Other destinations pass unchanged.
//! Run with `cargo run -p rusteron-media-driver --features static --example burst_loss -- 127.0.0.1:40123`.

use rusteron_media_driver::{AeronCError, AeronDriverContext, extensions::*};
use std::net::SocketAddr;

/// Gilbert-Elliott probabilities in parts per million, from zero to 1,000,000.
/// Each agent starts in the good state with an independent copy of `seed`.
#[derive(Clone, Copy, Debug)]
pub struct GilbertElliottConfig {
    pub seed: u64,
    pub good_to_bad: u32,
    pub bad_to_good: u32,
    pub good_loss: u32,
    pub bad_loss: u32,
}

/// Seeded burst loss for tests, with separate loss probabilities in two states.
///
/// Each batch samples loss in the current state, then the next state.
/// State is local to each agent.
pub struct GilbertElliottLoss {
    random: u64,
    bad: bool,
    transitions: [u64; 2],
    losses: [u64; 2],
}

impl GilbertElliottLoss {
    pub fn new(config: GilbertElliottConfig) -> Result<Self, AeronCError> {
        fn threshold(ppm: u32) -> Result<u64, AeronCError> {
            if ppm > 1_000_000 {
                return Err(AeronCError::with_message(
                    -1,
                    "loss probabilities must be at most 1,000,000 parts per million",
                ));
            }
            Ok((u64::from(ppm) << 32) / 1_000_000)
        }
        Ok(Self {
            random: config.seed,
            bad: false,
            transitions: [threshold(config.good_to_bad)?, threshold(config.bad_to_good)?],
            losses: [threshold(config.good_loss)?, threshold(config.bad_loss)?],
        })
    }

    #[inline]
    fn draw(&mut self) -> u64 {
        self.random = self.random.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.random;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        (value ^ (value >> 31)) >> 32
    }

    #[inline]
    pub fn should_drop(&mut self) -> bool {
        let index = usize::from(self.bad);
        let drop = self.draw() < self.losses[index];
        if self.draw() < self.transitions[index] {
            self.bad = !self.bad;
        }
        drop
    }
}

impl InterceptorStateFactory for GilbertElliottLoss {
    type State = GilbertElliottConfig;
    fn new(config: &Self::State, _: StrategyContext, _: TransportAffinity) -> Result<Self, AeronCError> {
        Self::new(*config)
    }
}
impl OutgoingInterceptor for GilbertElliottLoss {
    #[inline]
    fn send<'a>(&mut self, next: OutgoingNext<'a>, packet: OutgoingPacket<'a>) -> i32 {
        if self.should_drop() {
            packet.drop_as_sent()
        } else {
            next.send(packet)
        }
    }
}

/// Apply a loss policy to exactly one destination IP address and port.
/// IPv6 scope IDs must also match; IPv6 flow information is ignored.
///
/// Other destinations pass unchanged and do not advance the loss policy.
/// Install with `add_outgoing_interceptor_with_state`, passing a tuple of
/// the destination and the inner policy's construction state.
pub struct DestinationLoss<L> {
    destination: SocketAddr,
    loss: L,
}
impl<L> DestinationLoss<L> {
    pub fn new(destination: SocketAddr, loss: L) -> Self {
        Self { destination, loss }
    }
}
impl<L: InterceptorStateFactory> InterceptorStateFactory for DestinationLoss<L> {
    type State = (SocketAddr, L::State);
    fn new(state: &Self::State, context: StrategyContext, affinity: TransportAffinity) -> Result<Self, AeronCError> {
        Ok(Self::new(state.0, L::new(&state.1, context, affinity)?))
    }
}
impl<L: OutgoingInterceptor> OutgoingInterceptor for DestinationLoss<L> {
    const NOTIFICATIONS: bool = L::NOTIFICATIONS;

    fn on_notification(&mut self, notification: InterceptorNotification<'_>) -> i32 {
        self.loss.on_notification(notification)
    }

    #[inline]
    fn send<'a>(&mut self, next: OutgoingNext<'a>, packet: OutgoingPacket<'a>) -> i32 {
        let matches = match (self.destination, packet.address().to_socket_addr()) {
            (SocketAddr::V4(expected), Some(SocketAddr::V4(actual))) => expected == actual,
            (SocketAddr::V6(expected), Some(SocketAddr::V6(actual))) => {
                expected.ip() == actual.ip()
                    && expected.port() == actual.port()
                    && expected.scope_id() == actual.scope_id()
            }
            _ => false,
        };
        if matches {
            self.loss.send(next, packet)
        } else {
            next.send(packet)
        }
    }
}

pub fn configure(context: &AeronDriverContext, destination: SocketAddr) -> Result<(), AeronCError> {
    // Probabilities are parts per million. Bad periods last four batches on
    // average, with 60% loss. Good periods have 2% loss.
    let config = GilbertElliottConfig {
        seed: 7,
        good_to_bad: 20_000,
        bad_to_good: 250_000,
        good_loss: 20_000,
        bad_loss: 600_000,
    };
    context.add_outgoing_interceptor_with_state::<DestinationLoss<GilbertElliottLoss>>((destination, config))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let destination = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:40123".into())
        .parse()?;
    let driver =
        rusteron_media_driver::testing::EmbeddedDriver::launch_with(|context| configure(context, destination))?;
    println!("Driver directory: {}", driver.dir());
    println!("Burst loss applies to {destination}. Press Enter to stop.");
    std::io::stdin().read_line(&mut String::new())?;
    Ok(())
}
