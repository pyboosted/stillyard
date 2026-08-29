#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <tlhelp32.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef void *nvmlDevice_t;
typedef int nvmlReturn_t;

typedef struct nvmlMemory_st {
    uint64_t total;
    uint64_t free;
    uint64_t used;
} nvmlMemory_t;

typedef struct nvmlUtilization_st {
    unsigned int gpu;
    unsigned int memory;
} nvmlUtilization_t;

typedef struct nvmlProcessInfo_st {
    unsigned int pid;
    uint64_t usedGpuMemory;
    unsigned int gpuInstanceId;
    unsigned int computeInstanceId;
} nvmlProcessInfo_t;

static const char *GPU_UUID = "GPU-a1144c26-a15c-cba1-3b7a-870c755ef08a";
static const wchar_t *SUSPENDED_CHILD = L"stillyard-a05-child.exe";

static int suspended_child_exists(void) {
    HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
    PROCESSENTRY32W entry;
    if (snapshot == INVALID_HANDLE_VALUE) {
        return 1;
    }
    ZeroMemory(&entry, sizeof(entry));
    entry.dwSize = sizeof(entry);
    if (Process32FirstW(snapshot, &entry)) {
        do {
            if (_wcsicmp(entry.szExeFile, SUSPENDED_CHILD) == 0) {
                CloseHandle(snapshot);
                return 1;
            }
        } while (Process32NextW(snapshot, &entry));
    }
    CloseHandle(snapshot);
    return 0;
}

__declspec(dllexport) nvmlReturn_t nvmlInit_v2(void) { return 0; }
__declspec(dllexport) nvmlReturn_t nvmlShutdown(void) { return 0; }

__declspec(dllexport) nvmlReturn_t nvmlSystemGetDriverVersion(char *version,
                                                               unsigned int length) {
    const char *value = suspended_child_exists() ? "fixture-driver-B" : "fixture-driver-A";
    if (length <= strlen(value)) {
        return 7;
    }
    strcpy_s(version, length, value);
    return 0;
}

__declspec(dllexport) nvmlReturn_t nvmlDeviceGetCount_v2(unsigned int *count) {
    *count = 1;
    return 0;
}

__declspec(dllexport) nvmlReturn_t nvmlDeviceGetHandleByIndex_v2(unsigned int index,
                                                                  nvmlDevice_t *device) {
    if (index != 0) {
        return 6;
    }
    *device = (nvmlDevice_t)(uintptr_t)1;
    return 0;
}

__declspec(dllexport) nvmlReturn_t nvmlDeviceGetUUID(nvmlDevice_t device,
                                                      char *uuid,
                                                      unsigned int length) {
    (void)device;
    if (length <= strlen(GPU_UUID)) {
        return 7;
    }
    strcpy_s(uuid, length, GPU_UUID);
    return 0;
}

__declspec(dllexport) nvmlReturn_t nvmlDeviceGetMemoryInfo(nvmlDevice_t device,
                                                            nvmlMemory_t *memory) {
    (void)device;
    memory->total = UINT64_C(32768) * UINT64_C(1024) * UINT64_C(1024);
    memory->free = UINT64_C(24576) * UINT64_C(1024) * UINT64_C(1024);
    memory->used = memory->total - memory->free;
    return 0;
}

__declspec(dllexport) nvmlReturn_t nvmlDeviceGetUtilizationRates(
    nvmlDevice_t device, nvmlUtilization_t *utilization) {
    (void)device;
    utilization->gpu = 0;
    utilization->memory = 0;
    return 0;
}

__declspec(dllexport) nvmlReturn_t nvmlDeviceGetComputeRunningProcesses_v3(
    nvmlDevice_t device, unsigned int *count, nvmlProcessInfo_t *processes) {
    (void)device;
    (void)processes;
    *count = 0;
    return 0;
}
