# Run a command inside the kafka4php toolchain container (Windows PowerShell wrapper).
# Same behaviour as scripts/dev.sh - see that file for details.
#
#   scripts\dev.ps1 cargo test --workspace
#   scripts\dev.ps1 --with-kafka cargo test -p k4p-host
#   $env:PHP_VERSION = '8.5'; scripts\dev.ps1 php -v
#
# Env: PHP_VERSION (default 8.4), K4P_TTY=0|1 to force TTY allocation off/on.
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$phpVersion = if ($env:PHP_VERSION) { $env:PHP_VERSION } else { '8.4' }
$image = "kafka4php-dev:php$phpVersion"

$cmd = @($args)
$withKafka = $false
if ($cmd.Count -gt 0 -and $cmd[0] -eq '--with-kafka') {
    $withKafka = $true
    $cmd = @($cmd | Select-Object -Skip 1)
}

docker build --quiet -f "$root\docker\dev.Dockerfile" --build-arg "PHP_VERSION=$phpVersion" -t $image $root | Out-Null
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$runArgs = @('--rm', '-i')
$tty = if ($env:K4P_TTY) { $env:K4P_TTY -eq '1' } else {
    -not [Console]::IsInputRedirected -and -not [Console]::IsOutputRedirected
}
if ($tty) { $runArgs += '-t' }

if ($withKafka) {
    docker compose -f "$root\docker\compose.test.yml" up -d --wait | Out-Host
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $runArgs += @('--network', 'kafka4php-test', '-e', 'K4P_KAFKA_BOOTSTRAP=kafka:9092')
}

if ($cmd.Count -eq 0) { $cmd = @('bash') }

docker run @runArgs `
    -v "${root}:/work" `
    -v k4p-cargo-registry:/usr/local/cargo/registry `
    -v k4p-cargo-git:/usr/local/cargo/git `
    -v "k4p-target-php${phpVersion}:/work/target" `
    -w /work `
    -e CARGO_TERM_COLOR `
    -e RUST_BACKTRACE `
    -e RUST_LOG `
    $image @cmd
exit $LASTEXITCODE
