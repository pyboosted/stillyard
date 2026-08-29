use std::collections::BTreeMap;
use std::ffi::{CStr, c_char, c_void};
use std::ptr::null_mut;

use libloading::Library;

use super::{GpuEvidence, ProcessEvidence};
use crate::spec::canonical_gpu_uuid;

const NVML_SUCCESS: i32 = 0;
const NVML_ERROR_INSUFFICIENT_SIZE: i32 = 7;
const MAX_DEVICES: u32 = 64;
const MAX_COMPUTE_PROCESSES: u32 = 4096;
const MIB: u64 = 1024 * 1024;

type Device = *mut c_void;
type Return = i32;
type Init = unsafe extern "C" fn() -> Return;
type Shutdown = unsafe extern "C" fn() -> Return;
type SystemGetDriverVersion = unsafe extern "C" fn(*mut c_char, u32) -> Return;
type DeviceGetCount = unsafe extern "C" fn(*mut u32) -> Return;
type DeviceGetHandleByIndex = unsafe extern "C" fn(u32, *mut Device) -> Return;
type DeviceGetUuid = unsafe extern "C" fn(Device, *mut c_char, u32) -> Return;
type DeviceGetMemoryInfo = unsafe extern "C" fn(Device, *mut NvmlMemory) -> Return;
type DeviceGetUtilizationRates = unsafe extern "C" fn(Device, *mut NvmlUtilization) -> Return;
type DeviceGetComputeRunningProcesses =
    unsafe extern "C" fn(Device, *mut u32, *mut NvmlProcessInfo) -> Return;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct NvmlMemory {
    total: u64,
    free: u64,
    used: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct NvmlUtilization {
    gpu: u32,
    memory: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct NvmlProcessInfo {
    pid: u32,
    used_gpu_memory: u64,
    gpu_instance_id: u32,
    compute_instance_id: u32,
}

struct Functions {
    shutdown: Shutdown,
    system_get_driver_version: SystemGetDriverVersion,
    device_get_count: DeviceGetCount,
    device_get_handle_by_index: DeviceGetHandleByIndex,
    device_get_uuid: DeviceGetUuid,
    device_get_memory_info: DeviceGetMemoryInfo,
    device_get_utilization_rates: DeviceGetUtilizationRates,
    device_get_compute_running_processes: DeviceGetComputeRunningProcesses,
}

pub(crate) struct NvmlProvider {
    functions: Functions,
    // Function pointers remain valid only while the owning library is loaded.
    _library: Library,
}

impl NvmlProvider {
    pub(crate) fn load() -> Result<Self, String> {
        // SAFETY: loading uses the normal Windows DLL search contract. Every required symbol is
        // resolved before the initialized provider can escape this function.
        let library = unsafe { Library::new("nvml.dll") }
            .map_err(|error| format!("loading nvml.dll: {error}"))?;
        let init: Init = load_symbol(&library, b"nvmlInit_v2\0")?;
        let functions = Functions {
            shutdown: load_symbol(&library, b"nvmlShutdown\0")?,
            system_get_driver_version: load_symbol(&library, b"nvmlSystemGetDriverVersion\0")?,
            device_get_count: load_symbol(&library, b"nvmlDeviceGetCount_v2\0")?,
            device_get_handle_by_index: load_symbol(&library, b"nvmlDeviceGetHandleByIndex_v2\0")?,
            device_get_uuid: load_symbol(&library, b"nvmlDeviceGetUUID\0")?,
            device_get_memory_info: load_symbol(&library, b"nvmlDeviceGetMemoryInfo\0")?,
            device_get_utilization_rates: load_symbol(
                &library,
                b"nvmlDeviceGetUtilizationRates\0",
            )?,
            device_get_compute_running_processes: load_symbol(
                &library,
                b"nvmlDeviceGetComputeRunningProcesses_v3\0",
            )?,
        };
        // SAFETY: the symbol has the NVML initialization ABI and takes no pointers.
        check("nvmlInit_v2", unsafe { init() })?;
        Ok(Self {
            functions,
            _library: library,
        })
    }

    pub(crate) fn sample(
        &self,
        processes: &[ProcessEvidence],
    ) -> Result<BTreeMap<String, GpuEvidence>, String> {
        let driver_version = self.driver_version()?;
        let mut count = 0_u32;
        // SAFETY: count is a valid writable output.
        check("nvmlDeviceGetCount_v2", unsafe {
            (self.functions.device_get_count)(&raw mut count)
        })?;
        if count > MAX_DEVICES {
            return Err(format!("NVML topology exceeds {MAX_DEVICES} devices"));
        }
        let process_names = processes
            .iter()
            .map(|process| (process.pid, process.basename.as_str()))
            .collect::<BTreeMap<_, _>>();
        let mut devices = BTreeMap::new();
        for index in 0..count {
            let mut device = null_mut();
            // SAFETY: device is a valid writable handle output.
            check("nvmlDeviceGetHandleByIndex_v2", unsafe {
                (self.functions.device_get_handle_by_index)(index, &raw mut device)
            })?;
            if device.is_null() {
                return Err(format!("NVML returned a null handle for device {index}"));
            }
            let uuid = self.device_uuid(device)?;
            let mut memory = NvmlMemory::default();
            // SAFETY: memory has the documented v1 layout and is writable.
            check("nvmlDeviceGetMemoryInfo", unsafe {
                (self.functions.device_get_memory_info)(device, &raw mut memory)
            })?;
            let mut utilization = NvmlUtilization::default();
            // SAFETY: utilization has the documented layout and is writable.
            check("nvmlDeviceGetUtilizationRates", unsafe {
                (self.functions.device_get_utilization_rates)(device, &raw mut utilization)
            })?;
            let utilization_percent = u8::try_from(utilization.gpu)
                .ok()
                .filter(|value| *value <= 100)
                .ok_or_else(|| {
                    format!("NVML returned invalid GPU utilization {}", utilization.gpu)
                })?;
            let compute_processes = self
                .compute_pids(device)?
                .into_iter()
                .map(|pid| ProcessEvidence {
                    pid,
                    basename: process_names
                        .get(&pid)
                        .copied()
                        .unwrap_or("<unresolved>")
                        .to_owned(),
                })
                .collect();
            let evidence = GpuEvidence {
                uuid: uuid.clone(),
                driver_version: driver_version.clone(),
                free_memory_mb: memory.free / MIB,
                utilization_percent,
                compute_processes,
            };
            if devices.insert(uuid.clone(), evidence).is_some() {
                return Err(format!("NVML returned duplicate GPU UUID {uuid}"));
            }
        }
        Ok(devices)
    }

    fn driver_version(&self) -> Result<String, String> {
        let mut buffer = [0_i8; 80];
        // SAFETY: buffer is writable for the length passed.
        check("nvmlSystemGetDriverVersion", unsafe {
            (self.functions.system_get_driver_version)(buffer.as_mut_ptr(), buffer.len() as u32)
        })?;
        c_string("NVML driver version", &buffer)
    }

    fn device_uuid(&self, device: Device) -> Result<String, String> {
        let mut buffer = [0_i8; 96];
        // SAFETY: device is an enumerated handle and buffer is writable.
        check("nvmlDeviceGetUUID", unsafe {
            (self.functions.device_get_uuid)(device, buffer.as_mut_ptr(), buffer.len() as u32)
        })?;
        let uuid = c_string("NVML GPU UUID", &buffer)?;
        canonical_gpu_uuid(&uuid).map_err(|error| error.to_string())
    }

    fn compute_pids(&self, device: Device) -> Result<Vec<u32>, String> {
        let mut count = 0_u32;
        // SAFETY: a null output with count zero is the documented size query.
        let first = unsafe {
            (self.functions.device_get_compute_running_processes)(
                device,
                &raw mut count,
                null_mut(),
            )
        };
        if first == NVML_SUCCESS && count == 0 {
            return Ok(Vec::new());
        }
        if first != NVML_ERROR_INSUFFICIENT_SIZE {
            return Err(nvml_error("nvmlDeviceGetComputeRunningProcesses_v3", first));
        }
        for _ in 0..2 {
            if count > MAX_COMPUTE_PROCESSES {
                return Err(format!(
                    "NVML compute process inventory exceeds {MAX_COMPUTE_PROCESSES} entries"
                ));
            }
            let mut values = vec![NvmlProcessInfo::default(); count as usize];
            // SAFETY: values contains count writable entries with the documented v3 layout.
            let result = unsafe {
                (self.functions.device_get_compute_running_processes)(
                    device,
                    &raw mut count,
                    values.as_mut_ptr(),
                )
            };
            if result == NVML_SUCCESS {
                values.truncate(count as usize);
                return Ok(values.into_iter().map(|value| value.pid).collect());
            }
            if result != NVML_ERROR_INSUFFICIENT_SIZE {
                return Err(nvml_error(
                    "nvmlDeviceGetComputeRunningProcesses_v3",
                    result,
                ));
            }
        }
        Err("NVML compute process inventory changed during bounded retry".into())
    }
}

impl Drop for NvmlProvider {
    fn drop(&mut self) {
        // SAFETY: this provider successfully initialized NVML and owns the matching shutdown.
        let _ = unsafe { (self.functions.shutdown)() };
    }
}

fn load_symbol<T: Copy>(library: &Library, name: &[u8]) -> Result<T, String> {
    // SAFETY: callers provide the exact NVML ABI for each named function. The copied pointer
    // cannot outlive library because NvmlProvider stores the Library beside all pointers.
    unsafe { library.get::<T>(name) }
        .map(|symbol| *symbol)
        .map_err(|error| {
            let printable = name
                .strip_suffix(&[0])
                .map_or(name, |without_nul| without_nul);
            format!(
                "loading NVML symbol {}: {error}",
                String::from_utf8_lossy(printable)
            )
        })
}

fn check(operation: &str, result: Return) -> Result<(), String> {
    if result == NVML_SUCCESS {
        Ok(())
    } else {
        Err(nvml_error(operation, result))
    }
}

fn nvml_error(operation: &str, result: Return) -> String {
    format!("{operation} failed with NVML status {result}")
}

fn c_string(name: &str, buffer: &[c_char]) -> Result<String, String> {
    if !buffer.contains(&0) {
        return Err(format!("{name} was not NUL terminated"));
    }
    // SAFETY: the preceding check found a NUL within the live buffer.
    let value = unsafe { CStr::from_ptr(buffer.as_ptr()) }
        .to_str()
        .map_err(|error| format!("{name} is not UTF-8: {error}"))?;
    if value.is_empty() {
        return Err(format!("{name} is empty"));
    }
    Ok(value.to_owned())
}
