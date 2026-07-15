$N = 500
$start = Get-Date
1..$N | ForEach-Object -Parallel {
    Invoke-RestMethod -Method Post -Uri http://localhost:8080/jobs `
      -ContentType "application/json" `
      -Body '{"type":"send_email","payload":{"To":"bench@test.com","Subject":"bench"}}' | Out-Null
} -ThrottleLimit 20
Write-Host "Submitted $N jobs in $((Get-Date) - $start)"