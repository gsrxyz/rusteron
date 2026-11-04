#!/usr/bin/env bash
# rusteron's x86-64 lab: two Intel VMs in one region, in Azure resource group rusteron-lab,
# which exists only while this script runs. It creates both, copies this checkout, main's
# tree and the harness, runs harness/vm.sh's phases on both at once, copies the results to
# target/x86lab/results/<stamp>/<node>/after-<phase> after each phase, and deletes the group
# on any exit. Runs on macOS (bsdtar, caffeinate) with the az CLI logged in and
# ~/.ssh/id_rsa.pub as the VMs' key; summarise a bench.csv with analyse.py. It refuses to
# start unless more than 10 USD of free credit is left.
#
#   scripts/x86-lab/lab.sh                            every phase
#   LAB_PHASES="bootstrap build bench" scripts/x86-lab/lab.sh
#   LAB_ARMS="main impr" (only these harness arms), LAB_TESTS=workspace (only the
#   workspace tests) and LAB_EXTRAS=samples (also build the Java and Rust samples) pass
#   through to the VMs. LAB_NODES="name:region:size ..." replaces the two default nodes;
#   LAB_PAIR=1 puts each region's nodes in a placement group and, after the phases, runs
#   the cross-host UDP bench from each region's first node to its second; with LAB_LOCKSTEP=1
#   too, every phase finishes on all nodes before the next starts, and xhost8-<state> phases
#   run that bench between each pair in the state the phases before brought both hosts to;
#   xnet8-region runs from the first node to the first node of another region, the regions'
#   networks peered.
#   LAB_ZONE puts every VM in that availability zone, and LAB_DATA_DISK ("<sku>:<GiB>:<IOPS>:<MB/s> ...")
#   attaches data disks of those kinds to each VM
set -uo pipefail
export PYTHONWARNINGS=ignore::SyntaxWarning COPYFILE_DISABLE=1

# az 2.90 on Python 3.14 sometimes dies importing `requests` on two threads at once
# (`_DeadlockError`) before it sends anything; the same call then succeeds. Only that
# failure is retried. By path, not `command az`, because bash 3.2 execs a command run through
# `command` in place of a background subshell.
az_bin=$(type -P az) || { echo "az is not on PATH" >&2; exit 1; }
az() {
    local err rc attempt
    for attempt in 1 2 3 4 5; do
        rc=0
        # stdout passes through to the caller; stderr is kept to look for the deadlock
        { err=$("$az_bin" "$@" 2>&1 1>&3 3>&-); } 3>&1 || rc=$?
        if [[ -n $err ]]; then printf '%s\n' "$err" >&2; fi
        if ((rc == 0)) || [[ $err != *_DeadlockError* ]]; then return "$rc"; fi
        echo "az: import deadlock (attempt $attempt), retrying: az $1 ${2:-}" >&2
    done
    return "$rc"
}

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
group=rusteron-lab
user=$(id -un)
out=$repo/target/x86lab/results/$(date +%Y%m%d-%H%M%S)
# main's tree, for the arms that A/B against it, one per commit so a moved main is extracted afresh
main_sha=$(git -C "$repo" rev-parse main) || exit 1
main_tree=$repo/target/x86lab/rusteron-main-$main_sha
known=$out/known_hosts
read -ra nodes <<<"${LAB_NODES:-intel-a:northcentralus:Standard_D8s_v6 intel-b:northcentralus:Standard_D8s_v6}"
read -ra phases <<<"${LAB_PHASES:-bootstrap build bench test}"
# the regions the nodes are in, whose network watchers teardown removes
lab_regions=$(for n in "${nodes[@]}"; do IFS=: read -r _ r _ <<<"$n"; echo "$r"; done | sort -u | tr '\n' ' ')
ssh_opts=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$known"
    -o ConnectTimeout=15 -o ServerAliveInterval=30 -o ServerAliveCountMax=10)
created=0
before=?
# the network watchers, and whether their group existed, before this run: teardown leaves those alone
watchers_before=
watcher_group_before=true

log() { echo "[$(date +%T)] $*"; }

# the estimated free credit left on the first billing account's first billing profile, in USD
credit_left() {
    local api=https://management.azure.com/providers/Microsoft.Billing/billingAccounts acc profile
    acc=$(az rest --method get --url "$api?api-version=2024-04-01" --query "value[0].name" -o tsv) || return
    profile=$(az rest --method get --url "$api/$acc/billingProfiles?api-version=2024-04-01" --query "value[0].name" -o tsv) || return
    az rest --method get -o tsv --query properties.balanceSummary.estimatedBalance.value \
        --url "$api/$acc/billingProfiles/$profile/providers/Microsoft.Consumption/credits/balanceSummary?api-version=2023-05-01"
}

# seconds a phase may run on a VM
limit() {
    case $1 in
        bootstrap) echo 1800 ;;
        build) echo 4800 ;;
        bench) echo 3600 ;;
        abudp) echo 2400 ;;
        bench-pinned | bench-isolated | bench-huge | bench-1g | k8s | k8s-cpu | bench8 | archive8 | bench8-isolated) echo 3600 ;;
        bench8-tuned | bench8-tuned-nomit | bench8-tunedx | xhost8-* | xnet8-* | disks8 | diskbench8*) echo 3600 ;;
        archload8 | archburst8 | xarchload8) echo 7200 ;;
        xarcrtt8) echo 1800 ;;
        k8s8) echo 5400 ;;
        isolate | isolate8 | tune8 | tune8-nomit | tune8x | k3s-down) echo 600 ;;
        kernel) echo 1200 ;;
        test) echo 5400 ;;
    esac
}

# new_watchers < "<id> <location>" lines: the ids of the watchers in a lab region that were not
# there before this run (Azure ids compare case-insensitively)
new_watchers() {
    awk -v r=" $lab_regions " -v old=" ${watchers_before//$'\n'/ } " \
        'index(r, " " $2 " ") && !index(tolower(old), " " tolower($1) " ") { print $1 }'
}

teardown() {
    local rc=$? id attempt
    trap - EXIT HUP INT TERM
    if ((created)); then
        kill $(jobs -p) 2>/dev/null
        log "teardown: deleting $group"
        for _ in 1 2 3; do
            if az group delete -n "$group" --yes; then break; fi
            sleep 20
        done
        # the network watchers Azure added for our networks, and their group if this run created
        # it and that empties it; a watcher can show up in listings late, so look three times
        for attempt in 1 2 3; do
            id=$(az resource list -g NetworkWatcherRG --query "[?type=='Microsoft.Network/networkWatchers'].{id:id, l:location}" \
                -o tsv 2>/dev/null | new_watchers)
            if [[ -n $id ]]; then az resource delete --ids $id -o none; fi
            if ((attempt < 3)); then sleep 20; fi
        done
        if [[ $watcher_group_before == false && $(az group exists -n NetworkWatcherRG) == true &&
            $(az resource list -g NetworkWatcherRG --query 'length(@)' -o tsv) == 0 ]]; then
            az group delete -n NetworkWatcherRG --yes
        fi
        log "group $group exists: $(az group exists -n "$group")"
        log "resources in the subscription: $(az resource list --query 'length(@)' -o tsv) (before up: $before)"
        az resource list --query '[].[resourceGroup, type, name, location]' -o tsv
    fi
    exit "$rc"
}

# one network per region, so the nodes of a region share a subnet; with LAB_PAIR also a
# proximity placement group, so a pair sits as close together as Azure allows.
# create_network <region> <index>: each region's address space is its own, so they can peer
create_network() {
    local region=$1 net=10.$((60 + $2))
    az network nsg create -g "$group" -n "$region-nsg" -l "$region" -o none &&
        az network nsg rule create -g "$group" --nsg-name "$region-nsg" -n ssh-from-operator --priority 100 \
            --source-address-prefixes "$mine" --destination-port-ranges 22 --protocol Tcp --access Allow -o none &&
        az network vnet create -g "$group" -n "$region-vnet" -l "$region" --address-prefixes "$net.0.0/16" \
            --subnet-name s --subnet-prefixes "$net.0.0/24" --network-security-group "$region-nsg" -o none || return 1
    if [[ -n ${LAB_PAIR:-} ]]; then
        # no --zone here: az then demands --intent-vm-sizes, and the first zonal VM anchors the group anyway
        az ppg create -g "$group" -n "$region-ppg" -l "$region" -t Standard -o none || return 1
    fi
}

create_node() {
    local name=$1 region=$2 size=$3 ppg=() zone=() dsku dsize diops dmbps
    if [[ -n ${LAB_PAIR:-} ]]; then ppg=(--ppg "$region-ppg"); fi
    if [[ -n ${LAB_ZONE:-} ]]; then zone=(--zone "$LAB_ZONE"); fi
    az vm create -g "$group" -n "lab-$name" -l "$region" --size "$size" \
        --image Debian:debian-13:13-gen2:latest --admin-username "$user" --ssh-key-values "$key" \
        --vnet-name "$region-vnet" --subnet s --nsg "" --public-ip-address "$name-ip" --public-ip-sku Standard \
        --accelerated-networking true --os-disk-size-gb 64 --storage-sku Premium_LRS ${ppg[@]+"${ppg[@]}"} \
        ${zone[@]+"${zone[@]}"} --disk-controller-type NVMe --os-disk-delete-option Delete --nic-delete-option Delete -o none || return 1
    # LAB_DATA_DISK="<sku>:<GiB>:<IOPS>:<MB/s> ...", e.g. PremiumV2_LRS:256:3000:125 (Premium SSD v2
    # needs LAB_ZONE); vm.sh tells the disks apart by size, so each needs a size of its own
    local spec i=0
    for spec in ${LAB_DATA_DISK:-}; do
        IFS=: read -r dsku dsize diops dmbps <<<"$spec"
        i=$((i + 1))
        az disk create -g "$group" -n "lab-$name-data$i" -l "$region" ${zone[@]+"${zone[@]}"} --sku "$dsku" --size-gb "$dsize" \
            --disk-iops-read-write "$diops" --disk-mbps-read-write "$dmbps" -o none &&
            az vm disk attach -g "$group" --vm-name "lab-$name" --name "lab-$name-data$i" -o none || return 1
    done
}

# pair_bench <ping node> <pong node>: cross-host UDP, driven from the ping node over ssh
pair_bench() {
    local a=$1 b=$2 a_ip b_ip b_private rc=0
    a_ip=$(az vm show -d -g "$group" -n "lab-$a" --query publicIps -o tsv)
    b_ip=$(az vm show -d -g "$group" -n "lab-$b" --query publicIps -o tsv)
    b_private=$(az vm show -d -g "$group" -n "lab-$b" --query privateIps -o tsv)
    pair_keys "$a_ip" "$b_ip"
    log "$a -> $b: bench-xhost"
    ssh "${ssh_opts[@]}" "$user@$a_ip" \
        "LAB_PEER_IP=$b_private BENCH3_REPS='${BENCH3_REPS:-}' timeout 3600 /srv/x86lab/harness/vm.sh bench-xhost" \
        >"$out/$a/bench-xhost.log" 2>&1 || rc=$?
    fetch "$a_ip" "$out/$a/after-bench-xhost"
    log "$a -> $b: bench-xhost exit $rc"
    return "$rc"
}

# main at its own submodule commits, extracted once per commit into target/; a failed extraction
# stays in the .partial directory, so it is never mistaken for a whole tree
make_main_tree() {
    local sub tmp=$main_tree.partial
    if [[ -d $main_tree ]]; then return; fi
    mkdir -p "$tmp" && git -C "$repo" archive "$main_sha" | tar -x -C "$tmp" || return 1
    for sub in rusteron-client/aeron rusteron-archive/aeron rusteron-media-driver/aeron; do
        git -C "$repo/$sub" archive "$(git -C "$repo" rev-parse "$main_sha:$sub")" | tar -x -C "$tmp/$sub" || return 1
    done
    mv "$tmp" "$main_tree"
}

# what git tracks in this checkout (with the submodules), main's tree and the harness;
# no build output and no macOS prebuilt libraries
sync_to() {
    local ip=$1
    (cd "$repo" && git ls-files --recurse-submodules -z |
        tar --null -T - --no-xattrs --no-mac-metadata -czf - |
        ssh "${ssh_opts[@]}" "$user@$ip" 'mkdir -p /srv/x86lab/rusteron && tar -xzf - -C /srv/x86lab/rusteron') &&
        tar -C "$here" --no-xattrs --no-mac-metadata --exclude target -czf - harness |
        ssh "${ssh_opts[@]}" "$user@$ip" 'tar -xzf - -C /srv/x86lab' &&
        tar -C "$main_tree" --no-xattrs --no-mac-metadata -czf - . |
        ssh "${ssh_opts[@]}" "$user@$ip" 'mkdir -p /srv/x86lab/rusteron-main && tar -xzf - -C /srv/x86lab/rusteron-main'
}

fetch() {
    local ip=$1 dest=$2
    mkdir -p "$dest"
    ssh "${ssh_opts[@]}" "$user@$ip" 'tar -C /srv/x86lab -czf - results' | tar -xzf - -C "$dest"
}

# the node reachable, /srv/x86lab made and the trees synced to it
prepare_node() {
    local name=$1 ip=$2 dest=$out/$1
    mkdir -p "$dest"
    for _ in $(seq 60); do
        if ssh "${ssh_opts[@]}" "$user@$ip" true 2>/dev/null; then break; fi
        sleep 5
    done
    ssh "${ssh_opts[@]}" "$user@$ip" 'sudo install -d -o "$(id -un)" -g "$(id -gn)" /srv/x86lab' || return 1
    if ! sync_to "$ip" >"$dest/sync.log" 2>&1; then
        log "$name: sync failed"
        return 1
    fi
}

# run_phase <name> <ip> <phase> [env assignment...]: one vm.sh phase on one node, waiting out
# the reboot the isolate* and tune8* phases end with, then the results copied back
run_phase() {
    local name=$1 ip=$2 phase=$3 dest=$out/$1 rc=0
    shift 3
    log "$name: $phase"
    ssh "${ssh_opts[@]}" "$user@$ip" \
        "$* LAB_ARMS='${LAB_ARMS:-}' LAB_TESTS='${LAB_TESTS:-}' LAB_EXTRAS='${LAB_EXTRAS:-}' LAB_DATA_DISK='${LAB_DATA_DISK:-}' timeout $(limit "$phase") /srv/x86lab/harness/vm.sh $phase" \
        >"$dest/$phase.log" 2>&1 || rc=$?
    if [[ $phase == isolate* || $phase == tune8* || $phase == kernel ]]; then
        # the VM reboots a few seconds after the phase returns
        sleep 45
        for _ in $(seq 60); do
            if ssh "${ssh_opts[@]}" "$user@$ip" true 2>/dev/null; then break; fi
            sleep 5
        done
    fi
    fetch "$ip" "$dest/after-$phase"
    log "$name: $phase exit $rc"
    return "$rc"
}

# run_node <name> <ip>: every phase on one node, stopping at a failed bootstrap or build; a later
# phase that fails still lets the rest run, and the node then reports failure
run_node() {
    local name=$1 ip=$2 phase rc failed=0
    prepare_node "$name" "$ip" || return 1
    for phase in "${phases[@]}"; do
        rc=0
        run_phase "$name" "$ip" "$phase" || rc=$?
        if ((rc != 0)); then
            failed=1
            if [[ $phase == bootstrap || $phase == build ]]; then return "$rc"; fi
        fi
    done
    return "$failed"
}

# LAB_LOCKSTEP=1, with LAB_PAIR=1: each phase runs on every node at once and finishes on all of
# them before the next starts, so both hosts of a pair are in the same state; an xhost8-* phase
# runs from each region's first node, pinging its second
lockstep() {
    local spec name region size ip i j p pids=() failed=0 phase rc
    local names=() ips=() regs=() pairs=() xpairs=() use=()
    for spec in "${nodes[@]}"; do
        IFS=: read -r name region size <<<"$spec"
        ip=$(az vm show -d -g "$group" -n "lab-$name" --query publicIps -o tsv)
        log "$name ($size, $region): $ip"
        names+=("$name") ips+=("$ip") regs+=("$region")
        prepare_node "$name" "$ip" &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || failed=1; done
    ((failed == 0)) || return 1
    # each region's first two nodes, as "first second": the first pings, the second pongs
    for ((i = 0; i < ${#names[@]}; i++)); do
        for ((j = i + 1; j < ${#names[@]}; j++)); do
            if [[ ${regs[i]} == "${regs[j]}" && " ${pairs[*]-} " != *" $i:"* && " ${pairs[*]-} " != *":$j "* ]]; then
                pairs+=("$i:$j")
                pair_keys "${ips[i]}" "${ips[j]}" || return 1
                break
            fi
        done
    done
    # the first node and the first node in another region, for xnet8-region
    for ((j = 1; j < ${#names[@]}; j++)); do
        if [[ ${regs[j]} != "${regs[0]}" ]]; then
            xpairs+=("0:$j")
            pair_keys "${ips[0]}" "${ips[j]}" || return 1
            break
        fi
    done
    for phase in "${phases[@]}"; do
        pids=()
        if [[ $phase == xhost8* || $phase == xarc* || $phase == xnet8* ]]; then
            use=(${pairs[@]+"${pairs[@]}"})
            if [[ $phase == xnet8-region ]]; then use=(${xpairs[@]+"${xpairs[@]}"}); fi
            for p in ${use[@]+"${use[@]}"}; do
                i=${p%:*} j=${p#*:}
                run_phase "${names[i]}" "${ips[i]}" "$phase" "LAB_PEER_IP=$(az vm show -d -g "$group" -n "lab-${names[j]}" --query privateIps -o tsv)" &
                pids+=($!)
            done
        else
            for ((i = 0; i < ${#names[@]}; i++)); do
                run_phase "${names[i]}" "${ips[i]}" "$phase" &
                pids+=($!)
            done
        fi
        rc=0
        for p in "${pids[@]}"; do wait "$p" || rc=1; done
        if ((rc != 0)); then
            failed=1
            if [[ $phase == bootstrap || $phase == build ]]; then return 1; fi
        fi
    done
    return "$failed"
}

# pair_keys <ping ip> <pong ip>: the ping host may ssh to the pong host
pair_keys() {
    local pub
    ssh "${ssh_opts[@]}" "$user@$1" 'test -f ~/.ssh/id_ed25519 || ssh-keygen -q -t ed25519 -N "" -f ~/.ssh/id_ed25519' || return 1
    pub=$(ssh "${ssh_opts[@]}" "$user@$1" cat .ssh/id_ed25519.pub) || return 1
    ssh "${ssh_opts[@]}" "$user@$2" "echo '$pub' >>~/.ssh/authorized_keys"
}

main() {
    mkdir -p "$out"
    # only on free credit. The subscription has no spending limit, so past the credit a card pays
    local credit
    credit=$(credit_left)
    if ! [[ $credit =~ ^[0-9]+(\.[0-9]+)?$ ]] || ! awk -v c="$credit" 'BEGIN { exit !(c > 10) }'; then
        echo "free credit left: '${credit}' USD, at or below the 10 USD margin or unreadable: not creating anything" >&2
        exit 1
    fi
    log "free credit left: $credit USD"
    if [[ $(az group exists -n "$group") != false ]]; then
        echo "$group already exists: not created by this run, so not touched" >&2
        exit 1
    fi
    before=$(az resource list --query 'length(@)' -o tsv)
    watchers_before=$(az resource list --resource-type Microsoft.Network/networkWatchers --query '[].id' -o tsv) &&
        watcher_group_before=$(az group exists -n NetworkWatcherRG) || {
        echo "could not list the existing network watchers: not creating anything" >&2
        exit 1
    }
    { git -C "$repo" rev-parse HEAD; echo "$main_sha"; git -C "$repo" status --short; } >"$out/shas.txt"
    key=$HOME/.ssh/id_rsa.pub
    mine=$(curl -fsS https://api.ipify.org)/32
    # the Mac must not sleep while the lab runs
    if command -v caffeinate >/dev/null; then caffeinate -i -w $$ & fi
    make_main_tree || { echo "could not extract main's tree into $main_tree.partial" >&2; exit 1; }

    trap teardown EXIT HUP INT TERM
    created=1
    log "up: $group"
    az group create -n "$group" -l northcentralus -o none || exit 1
    local spec name region size pids=() failed=0 p regions=" " k=0 a b
    for spec in "${nodes[@]}"; do
        IFS=: read -r name region size <<<"$spec"
        if [[ $regions != *" $region "* ]]; then
            regions+="$region "
            create_network "$region" "$k" >"$out/network-$region.log" 2>&1 || { log "network $region failed"; exit 1; }
            k=$((k + 1))
        fi
    done
    # nodes in more than one region reach each other over global peering of their networks
    for a in $regions; do
        for b in $regions; do
            if [[ $a != "$b" ]]; then
                az network vnet peering create -g "$group" -n "$a-to-$b" --vnet-name "$a-vnet" --remote-vnet "$b-vnet" \
                    --allow-vnet-access -o none >>"$out/network-$a.log" 2>&1 || { log "peering $a to $b failed"; exit 1; }
            fi
        done
    done
    for spec in "${nodes[@]}"; do
        IFS=: read -r name region size <<<"$spec"
        create_node "$name" "$region" "$size" >"$out/create-$name.log" 2>&1 &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || failed=1; done
    if ((failed)); then
        log "a VM failed to create: see $out/create-*.log"
        exit 1
    fi

    if [[ -n ${LAB_LOCKSTEP:-} ]]; then
        lockstep || failed=1
        log "lab run finished (failed=$failed); results in $out"
        exit "$failed"
    fi
    pids=()
    for spec in "${nodes[@]}"; do
        IFS=: read -r name region size <<<"$spec"
        local ip
        ip=$(az vm show -d -g "$group" -n "lab-$name" --query publicIps -o tsv)
        log "$name ($size, $region): $ip"
        run_node "$name" "$ip" &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || failed=1; done
    if [[ -n ${LAB_PAIR:-} ]] && ((failed == 0)); then
        # in each region, the first node pings and the second pongs
        pids=()
        for region in $regions; do
            local pair=()
            for spec in "${nodes[@]}"; do
                IFS=: read -r name r size <<<"$spec"
                if [[ $r == "$region" ]]; then pair+=("$name"); fi
            done
            if ((${#pair[@]} == 2)); then
                pair_bench "${pair[0]}" "${pair[1]}" &
                pids+=($!)
            fi
        done
        for p in "${pids[@]}"; do wait "$p" || failed=1; done
    fi
    log "lab run finished (failed=$failed); results in $out"
    exit "$failed"
}

main
