//! Deterministic outgoing loss policies for transport tests.

use super::*;
use std::net::SocketAddr;

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
/// For each batch, sample loss in the current state, then sample the transition
/// for the next batch. SplitMix64 supplies two draws per batch, even for zero
/// or certain probabilities. Seed zero is valid. State is local to each agent.
/// Decisions allocate nothing and use no synchronization.
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
    fn should_drop(&mut self) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> GilbertElliottConfig {
        GilbertElliottConfig {
            seed: 7,
            good_to_bad: 200_000,
            bad_to_good: 300_000,
            good_loss: 100_000,
            bad_loss: 900_000,
        }
    }

    #[test]
    fn rejects_each_out_of_range_probability() {
        for index in 0..4 {
            let mut invalid = config();
            match index {
                0 => invalid.good_to_bad = 1_000_001,
                1 => invalid.bad_to_good = 1_000_001,
                2 => invalid.good_loss = 1_000_001,
                _ => invalid.bad_loss = 1_000_001,
            }
            assert!(GilbertElliottLoss::new(invalid).is_err());
        }
    }

    #[test]
    fn zero_and_certain_probabilities_control_both_states() {
        for seed in [0, 1, u64::MAX] {
            for (good_loss, bad_loss) in [(0, 0), (1_000_000, 1_000_000), (0, 1_000_000), (1_000_000, 0)] {
                let mut loss = GilbertElliottLoss::new(GilbertElliottConfig {
                    seed,
                    good_to_bad: 1_000_000,
                    bad_to_good: 1_000_000,
                    good_loss,
                    bad_loss,
                })
                .unwrap();
                crate::test_alloc::assert_no_heap_allocation(|| {
                    for index in 0..100 {
                        assert_eq!(
                            loss.should_drop(),
                            if index % 2 == 0 { good_loss != 0 } else { bad_loss != 0 }
                        );
                    }
                });
            }
            let mut good = GilbertElliottLoss::new(GilbertElliottConfig {
                seed,
                good_to_bad: 0,
                bad_to_good: 0,
                good_loss: 0,
                bad_loss: 1_000_000,
            })
            .unwrap();
            let mut bad = GilbertElliottLoss::new(GilbertElliottConfig {
                seed,
                good_to_bad: 1_000_000,
                bad_to_good: 0,
                good_loss: 0,
                bad_loss: 1_000_000,
            })
            .unwrap();
            assert!(!bad.should_drop());
            for _ in 0..100 {
                assert!(!good.should_drop());
                assert!(bad.should_drop());
            }
        }
    }

    #[test]
    fn seed_repeats_the_same_bursts() {
        let mut first = GilbertElliottLoss::new(config()).unwrap();
        let mut second = GilbertElliottLoss::new(config()).unwrap();
        let mut different = GilbertElliottLoss::new(GilbertElliottConfig { seed: 8, ..config() }).unwrap();
        let mut pattern = 0u64;
        let mut other = 0u64;
        crate::test_alloc::assert_no_heap_allocation(|| {
            for index in 0..64 {
                let drop = first.should_drop();
                assert_eq!(drop, second.should_drop());
                pattern |= u64::from(drop) << index;
                other |= u64::from(different.should_drop()) << index;
            }
        });
        assert_eq!(pattern, 0x0005_c7f0_0474_2804);
        assert_ne!(pattern, other);
    }
}
