//! Read-only startup diagnostics. This module does no packet work and changes no host settings.
use crate::{AeronDriverContext, AeronThreadingMode};
#[cfg(any(target_os = "linux", test))]
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct EnvironmentReport {
    pub threading_mode: AeronThreadingMode,
    pub directory: PathBuf,
    pub directory_exists: bool,
    pub file_page_size: usize,
    pub mtu_length: usize,
    pub initial_receiver_window: usize,
    pub allowed_cpus: Option<String>,
    pub cpu_quota: Option<String>,
    pub mount: Option<MountReport>,
    pub hugepages: Vec<HugepageReport>,
    pub threads: Vec<ThreadPlacement>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountReport {
    pub directory: PathBuf,
    pub filesystem: String,
    pub options: String,
}
#[derive(Debug, Clone)]
pub struct HugepageReport {
    pub size_kib: u64,
    pub free_pages: Option<u64>,
    pub cgroup_limit: Option<String>,
    pub cgroup_usage: Option<String>,
}
#[derive(Debug, Clone)]
pub struct ThreadPlacement {
    pub name: String,
    pub allowed_cpus: Option<String>,
}

/// Copy the driver's settings and the process's current placement and memory limits.
/// Call before startup and again after agents start. Missing Linux files produce
/// `None`. The report does not test directory writability or reserve hugepages.
pub fn inspect_environment(context: &AeronDriverContext) -> EnvironmentReport {
    let directory = PathBuf::from(context.get_dir_owned());
    #[allow(unused_mut)]
    let mut report = EnvironmentReport {
        threading_mode: context.get_threading_mode(),
        directory_exists: directory.exists(),
        directory,
        file_page_size: context.get_file_page_size(),
        mtu_length: context.get_mtu_length(),
        initial_receiver_window: context.get_rcv_initial_window_length(),
        allowed_cpus: None,
        cpu_quota: None,
        mount: None,
        hugepages: Vec::new(),
        threads: Vec::new(),
    };
    #[cfg(target_os = "linux")]
    {
        report.allowed_cpus =
            read("/proc/self/status").and_then(|text| status_field(&text, "Cpus_allowed_list").map(str::to_owned));
        let cgroup = read("/proc/self/mountinfo")
            .zip(read("/proc/self/cgroup"))
            .and_then(|(mounts, groups)| cgroup_directory(&mounts, &groups));
        report.cpu_quota = cgroup.as_ref().and_then(|directory| read(directory.join("cpu.max")));
        report.mount = read("/proc/self/mountinfo").and_then(|text| mount_for(&report.directory, &text));
        for (size_kib, size_label) in [(2048, "2MB"), (1048576, "1GB")] {
            report.hugepages.push(HugepageReport {
                size_kib,
                free_pages: read(&format!(
                    "/sys/kernel/mm/hugepages/hugepages-{size_kib}kB/free_hugepages"
                ))
                .and_then(|value| value.parse().ok()),
                cgroup_limit: cgroup
                    .as_ref()
                    .and_then(|directory| read(directory.join(format!("hugetlb.{size_label}.max")))),
                cgroup_usage: cgroup
                    .as_ref()
                    .and_then(|directory| read(directory.join(format!("hugetlb.{size_label}.current")))),
            });
        }
        if let Ok(threads) = std::fs::read_dir("/proc/self/task") {
            for thread in threads.flatten() {
                let name = read(thread.path().join("comm"));
                let status = read(thread.path().join("status"));
                if let Some(name) = name {
                    report.threads.push(ThreadPlacement {
                        name,
                        allowed_cpus: status
                            .and_then(|text| status_field(&text, "Cpus_allowed_list").map(str::to_owned)),
                    });
                }
            }
            report.threads.sort_by(|a, b| a.name.cmp(&b.name));
        }
    }
    report
}
#[cfg(target_os = "linux")]
fn read(path: impl AsRef<Path>) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|text| text.trim().to_owned())
}
#[cfg(any(target_os = "linux", test))]
fn status_field<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key == name).then(|| value.trim())
    })
}
#[cfg(any(target_os = "linux", test))]
fn mount_for(directory: &Path, text: &str) -> Option<MountReport> {
    fn unescape(text: &str) -> String {
        text.replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\")
    }
    text.lines()
        .filter_map(|line| {
            let (left, right) = line.split_once(" - ")?;
            let fields: Vec<_> = left.split_whitespace().collect();
            let mount = PathBuf::from(unescape(fields.get(4)?));
            if !directory.starts_with(&mount) {
                return None;
            }
            let mut filesystem = right.split_whitespace();
            let kind = filesystem.next()?.to_owned();
            filesystem.next()?;
            let options = format!("{},{}", fields.get(5)?, filesystem.next()?);
            Some(MountReport {
                directory: mount,
                filesystem: kind,
                options,
            })
        })
        .max_by_key(|mount| mount.directory.as_os_str().len())
}
#[cfg(any(target_os = "linux", test))]
fn cgroup_directory(mounts: &str, groups: &str) -> Option<PathBuf> {
    let group = Path::new(groups.lines().find_map(|line| line.strip_prefix("0::"))?);
    if group
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return None;
    }
    mounts.lines().find_map(|line| {
        let (left, right) = line.split_once(" - ")?;
        if right.split_whitespace().next()? != "cgroup2" {
            return None;
        }
        let fields: Vec<_> = left.split_whitespace().collect();
        let relative = group.strip_prefix(fields.get(3)?).ok()?;
        Some(PathBuf::from(*fields.get(4)?).join(relative))
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cgroup_paths_follow_the_process_instead_of_the_mount_root() {
        let mounts = "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw";
        assert_eq!(
            cgroup_directory(mounts, "0::/app/worker"),
            Some(PathBuf::from("/sys/fs/cgroup/app/worker"))
        );
        assert!(cgroup_directory(mounts, "0::/../outside").is_none());
        assert!(cgroup_directory("invalid", "invalid").is_none());
    }
    #[test]
    fn report_reads_context_configuration() {
        let context = AeronDriverContext::new().unwrap();
        let report = inspect_environment(&context);
        assert_eq!(report.threading_mode, context.get_threading_mode());
        assert_eq!(report.file_page_size, context.get_file_page_size());
    }
    #[test]
    fn mount_selection_uses_path_components_and_unescapes_spaces() {
        let text =
            "1 0 0:1 / / rw - ext4 /dev/root rw\n2 1 0:2 / /dev/huge\\040pages rw - hugetlbfs none rw,pagesize=2M\n";
        assert_eq!(
            mount_for(Path::new("/dev/huge pages/aeron"), text).unwrap().filesystem,
            "hugetlbfs"
        );
        assert_eq!(mount_for(Path::new("/device/aeron"), text).unwrap().filesystem, "ext4");
        assert!(mount_for(Path::new("relative"), "malformed").is_none());
        assert_eq!(
            status_field("Name:\ttest\nCpus_allowed_list:\t2-5\n", "Cpus_allowed_list"),
            Some("2-5")
        );
        assert_eq!(status_field("Name: test", "Missing"), None);
    }
}
