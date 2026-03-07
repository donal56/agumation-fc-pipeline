param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$wrapperDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$wrapperDir\run_stage.ps1" "hardsub" @Args
