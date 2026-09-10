//! Whole-device guest I/O pressure; partitions never duplicate their parent.
//! https://docs.kernel.org/admin-guide/iostats.html
use super::{ComponentEvidence, ComponentValue};
use std::collections::BTreeMap;
use std::io::{self, Read};
use std::os::unix::fs::MetadataExt;

#[derive(Clone)]
struct Counter {
    inode: u64,
    active: u64,
    weighted: u64,
    inflight: u64,
}
type Counters = BTreeMap<(u32, u32), Counter>;

fn counters() -> io::Result<Counters> {
    let mut text = String::new();
    std::fs::File::open("/proc/diskstats")?
        .take(1_048_577)
        .read_to_string(&mut text)?;
    if text.len() > 1_048_576 {
        return Err(io::Error::other("disk counters exceed byte bound"));
    }
    let mut result = BTreeMap::new();
    for line in text.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 14 {
            return Err(io::Error::other("incomplete disk counters"));
        }
        let number = |i: usize| fields[i].parse::<u64>().map_err(io::Error::other);
        let key = (
            u32::try_from(number(0)?).map_err(io::Error::other)?,
            u32::try_from(number(1)?).map_err(io::Error::other)?,
        );
        let path = std::path::PathBuf::from(format!("/sys/dev/block/{}:{}", key.0, key.1));
        if path.join("partition").try_exists()? {
            continue;
        }
        let metadata = std::fs::metadata(&path)?;
        if result
            .insert(
                key,
                Counter {
                    inode: metadata.ino(),
                    inflight: number(11)?,
                    active: number(12)?,
                    weighted: number(13)?,
                },
            )
            .is_some()
            || result.len() > 128
        {
            return Err(io::Error::other(
                "disk inventory duplicate or outside supported bounds",
            ));
        }
    }
    if result.is_empty() {
        return Err(io::Error::other("whole-device disk inventory unavailable"));
    }
    Ok(result)
}

#[derive(Default)]
pub(crate) struct DiskUtilizationSampler {
    previous: Option<(u64, Counters)>,
}
impl DiskUtilizationSampler {
    pub(crate) fn sample(&mut self, wall: i64, monotonic: u64) -> ComponentEvidence<u8> {
        let value = match counters() {
            Err(error) => {
                self.reset();
                ComponentValue::Error(error.to_string())
            }
            Ok(current) => {
                let old = self.previous.replace((monotonic, current));
                match old {
                    None => ComponentValue::Unavailable("warming_up".into()),
                    Some((clock, old)) => {
                        utilization(clock, &old, monotonic, &self.previous.as_ref().unwrap().1)
                    }
                }
            }
        };
        ComponentEvidence {
            captured_unix_millis: wall,
            captured_monotonic_millis: monotonic,
            value,
        }
    }
    pub(crate) fn reset(&mut self) {
        self.previous = None;
    }
}
fn utilization(
    previous_time: u64,
    previous: &Counters,
    now: u64,
    current: &Counters,
) -> ComponentValue<u8> {
    let Some(elapsed) = now.checked_sub(previous_time).filter(|n| *n > 0) else {
        return ComponentValue::Unavailable("disk_clock_interval_empty_or_reset".into());
    };
    if previous.keys().ne(current.keys()) {
        return ComponentValue::Unavailable("disk_topology_changed".into());
    }
    let mut pressure = 0_u128;
    for (key, new) in current {
        let old = &previous[key];
        if old.inode != new.inode {
            return ComponentValue::Unavailable("disk_identity_changed".into());
        }
        let (Some(active), Some(weighted)) = (
            new.active.checked_sub(old.active),
            new.weighted.checked_sub(old.weighted),
        ) else {
            return ComponentValue::Unavailable("disk_counter_reset".into());
        };
        // Busy intervals can be undercounted by modern io_ticks. Weighted I/O
        // time and outstanding requests conservatively prevent a false idle.
        let busy = if old.inflight > 0 || new.inflight > 0 {
            elapsed
        } else {
            active.max(weighted).min(elapsed)
        };
        pressure = pressure.max((u128::from(busy) * 100).div_ceil(u128::from(elapsed)));
    }
    ComponentValue::Available(pressure as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn linux_disk_detects_inflight_pressure_reuse_and_counter_regression() {
        let old: Counters = [(
            (8, 0),
            Counter {
                inode: 1,
                active: 10,
                weighted: 10,
                inflight: 0,
            },
        )]
        .into();
        let mut new = old.clone();
        new.get_mut(&(8, 0)).unwrap().weighted = 60;
        assert_eq!(
            utilization(100, &old, 200, &new),
            ComponentValue::Available(50)
        );
        new.get_mut(&(8, 0)).unwrap().inflight = 1;
        assert_eq!(
            utilization(100, &old, 200, &new),
            ComponentValue::Available(100)
        );
        new.get_mut(&(8, 0)).unwrap().inode = 2;
        assert!(matches!(
            utilization(100, &old, 200, &new),
            ComponentValue::Unavailable(_)
        ));
        new.get_mut(&(8, 0)).unwrap().inode = 1;
        new.get_mut(&(8, 0)).unwrap().active = 0;
        assert!(matches!(
            utilization(100, &old, 200, &new),
            ComponentValue::Unavailable(_)
        ));
    }
}
