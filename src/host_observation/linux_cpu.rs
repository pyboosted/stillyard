//! Linux VM CPU accounting: /proc/stat, excluding guest's duplicate counters.
//! Field definitions: https://docs.kernel.org/filesystems/proc.html
use super::{ComponentEvidence, ComponentValue};
use std::io::{self, Read};

struct Counters {
    times: [u64; 8],
    cpus: Vec<u32>,
}

fn parse(text: &str) -> io::Result<Counters> {
    let mut lines = text.lines();
    let mut fields = lines
        .next()
        .ok_or_else(|| io::Error::other("missing CPU counters"))?
        .split_whitespace();
    if fields.next() != Some("cpu") {
        return Err(io::Error::other("missing aggregate CPU counters"));
    }
    let mut times = [0; 8];
    for counter in &mut times {
        *counter = fields
            .next()
            .ok_or_else(|| io::Error::other("incomplete CPU counters"))?
            .parse()
            .map_err(io::Error::other)?;
    }
    let mut cpus = Vec::new();
    for line in lines {
        let name = line.split_whitespace().next().unwrap_or("");
        if let Some(number) = name.strip_prefix("cpu") {
            let number = number.parse().map_err(io::Error::other)?;
            if cpus.last().is_some_and(|last| last >= &number) {
                return Err(io::Error::other("unordered CPU inventory"));
            }
            cpus.push(number);
        }
    }
    if cpus.is_empty() || cpus.len() > 4096 {
        return Err(io::Error::other("CPU inventory outside supported bounds"));
    }
    Ok(Counters { times, cpus })
}

#[derive(Default)]
pub(crate) struct CpuUtilizationSampler {
    previous: Option<Counters>,
}

impl CpuUtilizationSampler {
    pub(crate) fn sample(&mut self, wall: i64, monotonic: u64) -> ComponentEvidence<u8> {
        let counters = (|| {
            let mut text = String::new();
            std::fs::File::open("/proc/stat")?
                .take(1_048_577)
                .read_to_string(&mut text)?;
            if text.len() > 1_048_576 {
                return Err(io::Error::other("CPU counters exceed byte bound"));
            }
            parse(&text)
        })();
        ComponentEvidence {
            captured_unix_millis: wall,
            captured_monotonic_millis: monotonic,
            value: self.update(counters),
        }
    }
    fn update(&mut self, current: io::Result<Counters>) -> ComponentValue<u8> {
        let current = match current {
            Ok(current) => current,
            Err(error) => {
                self.reset();
                return ComponentValue::Error(error.to_string());
            }
        };
        let previous = self.previous.replace(current);
        let current = self.previous.as_ref().unwrap();
        let Some(previous) = previous else {
            return ComponentValue::Unavailable("warming_up".into());
        };
        if current.cpus != previous.cpus {
            return ComponentValue::Unavailable("cpu_topology_changed".into());
        }
        let mut deltas = [0_u64; 8];
        for ((delta, new), old) in deltas.iter_mut().zip(current.times).zip(previous.times) {
            let Some(value) = new.checked_sub(old) else {
                return ComponentValue::Unavailable("cpu_counter_reset".into());
            };
            *delta = value;
        }
        let total: u128 = deltas.into_iter().map(u128::from).sum();
        if total == 0 {
            return ComponentValue::Unavailable("cpu_counter_interval_empty".into());
        }
        let idle = u128::from(deltas[3]) + u128::from(deltas[4]);
        ComponentValue::Available(((total - idle) * 100).div_ceil(total) as u8)
    }
    pub(crate) fn reset(&mut self) {
        self.previous = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn linux_cpu_avoids_guest_double_count_and_fences_regressions_and_hotplug() {
        let mut sampler = CpuUtilizationSampler::default();
        assert!(matches!(
            sampler.update(parse("cpu 10 0 0 10 0 0 0 0 5 0\ncpu0 0")),
            ComponentValue::Unavailable(_)
        ));
        assert_eq!(
            sampler.update(parse("cpu 20 0 0 20 0 0 0 0 10 0\ncpu0 0")),
            ComponentValue::Available(50)
        );
        assert!(matches!(
            sampler.update(parse("cpu 30 0 0 30 0 0 0 0\ncpu0 0\ncpu1 0")),
            ComponentValue::Unavailable(_)
        ));
        assert!(matches!(
            sampler.update(parse("cpu 20 0 0 40 0 0 0 0\ncpu0 0\ncpu1 0")),
            ComponentValue::Unavailable(_)
        ));
        assert!(parse("cpu 1 2 3").is_err());
        assert!(matches!(
            sampler.update(Err(io::Error::other("lost procfs"))),
            ComponentValue::Error(_)
        ));
        assert!(matches!(
            sampler.update(parse("cpu 50 0 0 50 0 0 0 0\ncpu0 0")),
            ComponentValue::Unavailable(_)
        ));
    }
}
