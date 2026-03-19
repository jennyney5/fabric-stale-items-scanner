<#
.SYNOPSIS
    Fabric / Power BI Stale Items Scanner
.DESCRIPTION
    Scans all reports and semantic models across your tenant to find items
    whose most recent refresh is more than 30 days ago. Identifies upstream
    and downstream dependencies for deletion candidates and produces an HTML report.
    Works with both Microsoft Fabric and Power BI-only environments.
.NOTES
    Prerequisites: Az.Accounts module (Install-Module Az.Accounts)
    Usage: .\FabricStaleItemScanner.ps1
#>

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
$StaleThresholdDays = 30
$PbiApi = "https://api.powerbi.com/v1.0/myorg"
$Scope = "https://analysis.windows.net/powerbi/api/.default"
$OutputFile = Join-Path $PSScriptRoot "fabric_stale_items_report.html"

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
function Get-FabricToken {
    Connect-AzAccount -ErrorAction Stop | Out-Null
    $tokenObj = Get-AzAccessToken -ResourceUrl "https://analysis.windows.net/powerbi/api" -ErrorAction Stop
    # Az.Accounts 4.x+ returns Token as SecureString
    if ($tokenObj.Token -is [securestring]) {
        return [System.Net.NetworkCredential]::new("", $tokenObj.Token).Password
    }
    return $tokenObj.Token
}

function Get-AuthHeaders {
    param([string]$Token)
    return @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/json"
    }
}

# ---------------------------------------------------------------------------
# Paginated GET helper
# ---------------------------------------------------------------------------
function Get-Paginated {
    param(
        [string]$Url,
        [hashtable]$Headers,
        [string]$Key = "value"
    )
    $items = [System.Collections.Generic.List[object]]::new()
    while ($Url) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
            $data = $resp.Content | ConvertFrom-Json
            $pageItems = $data.$Key
            if ($pageItems) {
                foreach ($item in $pageItems) { $items.Add($item) }
            }
            $Url = if ($data.continuationUri) { $data.continuationUri }
                   elseif ($data.'@odata.nextLink') { $data.'@odata.nextLink' }
                   else { $null }
        }
        catch {
            if ($_.Exception.Response.StatusCode -eq 429) {
                $retry = 30
                $retryHeader = $_.Exception.Response.Headers["Retry-After"]
                if ($retryHeader) { $retry = [int]$retryHeader }
                Write-Host "  Rate limited - waiting ${retry}s ..."
                Start-Sleep -Seconds $retry
                continue
            }
            throw
        }
    }
    return $items
}

# ---------------------------------------------------------------------------
# Data retrieval
# ---------------------------------------------------------------------------
function Get-FabricWorkspaces {
    param([hashtable]$Headers)
    $url = "$PbiApi/groups"
    return Get-Paginated -Url $url -Headers $Headers
}

function Get-FabricSemanticModels {
    param([string]$WorkspaceId, [hashtable]$Headers)
    $url = "$PbiApi/groups/$WorkspaceId/datasets"
    return Get-Paginated -Url $url -Headers $Headers
}

function Get-FabricReports {
    param([string]$WorkspaceId, [hashtable]$Headers)
    $url = "$PbiApi/groups/$WorkspaceId/reports"
    return Get-Paginated -Url $url -Headers $Headers
}

function Get-RefreshHistory {
    param([string]$WorkspaceId, [string]$DatasetId, [hashtable]$Headers)
    $url = "$PbiApi/groups/$WorkspaceId/datasets/$DatasetId/refreshes?`$top=1"
    try {
        $resp = Invoke-WebRequest -Uri $url -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
        $data = $resp.Content | ConvertFrom-Json
        return @($data.value)
    }
    catch {
        if ($_.Exception.Response.StatusCode -eq 429) {
            $retry = 30
            $retryHeader = $_.Exception.Response.Headers["Retry-After"]
            if ($retryHeader) { $retry = [int]$retryHeader }
            Start-Sleep -Seconds $retry
            try {
                $resp = Invoke-WebRequest -Uri $url -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
                $data = $resp.Content | ConvertFrom-Json
                return @($data.value)
            }
            catch { return @() }
        }
        return @()
    }
}

function Invoke-WorkspaceScanForLineage {
    param([string[]]$WorkspaceIds, [hashtable]$Headers)

    $results = @{}
    # Process in batches of 100 (API limit)
    for ($i = 0; $i -lt $WorkspaceIds.Count; $i += 100) {
        $batch = $WorkspaceIds[$i..([Math]::Min($i + 99, $WorkspaceIds.Count - 1))]
        $scanUrl = "$PbiApi/admin/workspaces/getInfo?lineage=true&datasourceDetails=true&datasetSchema=false&datasetExpressions=false&getArtifactUsers=false"
        $body = @{ workspaces = $batch } | ConvertTo-Json

        try {
            $resp = Invoke-WebRequest -Uri $scanUrl -Headers $Headers -Method Post -Body $body -UseBasicParsing -ErrorAction Stop
            if ($resp.StatusCode -ne 202) {
                Write-Host "  Scanner initiation failed ($($resp.StatusCode)) for batch starting at $i"
                continue
            }
            $scanData = $resp.Content | ConvertFrom-Json
            $scanId = $scanData.id
            if (-not $scanId) { continue }

            # Poll for scan completion
            $statusUrl = "$PbiApi/admin/workspaces/scanStatus/$scanId"
            for ($poll = 0; $poll -lt 60; $poll++) {
                Start-Sleep -Seconds 5
                try {
                    $statusResp = Invoke-WebRequest -Uri $statusUrl -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
                    $statusData = $statusResp.Content | ConvertFrom-Json
                    if ($statusData.status -eq "Succeeded") { break }
                }
                catch {
                    if ($_.Exception.Response.StatusCode -eq 429) {
                        $retry = 30
                        $retryHeader = $_.Exception.Response.Headers["Retry-After"]
                        if ($retryHeader) { $retry = [int]$retryHeader }
                        Start-Sleep -Seconds $retry
                    }
                }
            }

            # Retrieve scan result
            $resultUrl = "$PbiApi/admin/workspaces/scanResult/$scanId"
            $resultResp = Invoke-WebRequest -Uri $resultUrl -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
            $resultData = $resultResp.Content | ConvertFrom-Json
            foreach ($ws in $resultData.workspaces) {
                $results[$ws.id] = $ws
            }
        }
        catch {
            if ($_.Exception.Response.StatusCode -eq 429) {
                $retry = 60
                $retryHeader = $_.Exception.Response.Headers["Retry-After"]
                if ($retryHeader) { $retry = [int]$retryHeader }
                Write-Host "  Scanner rate limited - waiting ${retry}s ..."
                Start-Sleep -Seconds $retry
                # Retry the batch
                try {
                    $resp = Invoke-WebRequest -Uri $scanUrl -Headers $Headers -Method Post -Body $body -UseBasicParsing -ErrorAction Stop
                    if ($resp.StatusCode -eq 202) {
                        $scanData = $resp.Content | ConvertFrom-Json
                        $scanId = $scanData.id
                        if ($scanId) {
                            $statusUrl = "$PbiApi/admin/workspaces/scanStatus/$scanId"
                            for ($poll = 0; $poll -lt 60; $poll++) {
                                Start-Sleep -Seconds 5
                                try {
                                    $statusResp = Invoke-WebRequest -Uri $statusUrl -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
                                    $statusData = $statusResp.Content | ConvertFrom-Json
                                    if ($statusData.status -eq "Succeeded") { break }
                                }
                                catch { }
                            }
                            $resultUrl = "$PbiApi/admin/workspaces/scanResult/$scanId"
                            $resultResp = Invoke-WebRequest -Uri $resultUrl -Headers $Headers -Method Get -UseBasicParsing -ErrorAction Stop
                            $resultData = $resultResp.Content | ConvertFrom-Json
                            foreach ($ws in $resultData.workspaces) {
                                $results[$ws.id] = $ws
                            }
                        }
                    }
                }
                catch {
                    Write-Host "  Scanner error: $_"
                }
            }
            else {
                Write-Host "  Scanner error: $_"
            }
        }
    }
    return $results
}

# ---------------------------------------------------------------------------
# Lineage extraction helpers
# ---------------------------------------------------------------------------
function Get-ItemDependencies {
    param(
        [hashtable]$ScanData,
        [string]$ItemId,
        [string]$ItemType
    )
    $upstream = [System.Collections.Generic.List[object]]::new()
    $downstream = [System.Collections.Generic.List[object]]::new()

    foreach ($wsId in $ScanData.Keys) {
        $wsData = $ScanData[$wsId]
        $wsName = if ($wsData.name) { $wsData.name } else { $wsId }

        # Check datasets (semantic models)
        foreach ($ds in $wsData.datasets) {
            $dsId = $ds.id
            $dsName = $ds.name

            if ($dsId -eq $ItemId) {
                foreach ($source in $ds.upstreamDataflows) {
                    $upstream.Add(@{
                        name      = if ($source.targetDataflowId) { $source.targetDataflowId } else { "Unknown" }
                        type      = "Dataflow"
                        workspace = $wsName
                    })
                }
                foreach ($source in $ds.upstreamDatasets) {
                    $upstream.Add(@{
                        name      = if ($source.targetDatasetId) { $source.targetDatasetId } else { "Unknown" }
                        type      = "SemanticModel"
                        workspace = $wsName
                    })
                }
            }

            foreach ($upDs in $ds.upstreamDatasets) {
                if ($upDs.targetDatasetId -eq $ItemId) {
                    $downstream.Add(@{
                        name      = $dsName
                        id        = $dsId
                        type      = "SemanticModel"
                        workspace = $wsName
                    })
                }
            }
        }

        # Check reports
        foreach ($rpt in $wsData.reports) {
            $rptId = $rpt.id
            $rptName = $rpt.name
            $rptDatasetId = $rpt.datasetId

            if ($ItemType -eq "SemanticModel" -and $rptDatasetId -eq $ItemId) {
                $downstream.Add(@{
                    name      = $rptName
                    id        = $rptId
                    type      = "Report"
                    workspace = $wsName
                })
            }
            if ($ItemType -eq "Report" -and $rptId -eq $ItemId) {
                if ($rptDatasetId) {
                    $upstream.Add(@{
                        name      = $rptDatasetId
                        type      = "SemanticModel"
                        workspace = $wsName
                    })
                }
            }
        }
    }

    return @{
        upstream   = $upstream
        downstream = $downstream
    }
}

function Get-NameLookup {
    param([hashtable]$ScanData)
    $lookup = @{}
    foreach ($wsData in $ScanData.Values) {
        foreach ($ds in $wsData.datasets) {
            if ($ds.id) { $lookup[$ds.id] = $ds.name }
        }
        foreach ($rpt in $wsData.reports) {
            if ($rpt.id) { $lookup[$rpt.id] = $rpt.name }
        }
        foreach ($df in $wsData.dataflows) {
            if ($df.objectId) { $lookup[$df.objectId] = $df.name }
        }
    }
    return $lookup
}

# ---------------------------------------------------------------------------
# HTML report generation
# ---------------------------------------------------------------------------
function New-HtmlReport {
    param(
        [array]$Candidates,
        [array]$AllItems,
        [datetime]$Now
    )

    # --- Workspace summary: identify workspaces that ONLY contain stale items ---
    $wsTotalItems = @{}   # workspace_id -> total item count
    $wsNames = @{}        # workspace_id -> workspace name
    foreach ($item in $AllItems) {
        $wsTotalItems[$item.workspace_id] = ($wsTotalItems[$item.workspace_id] -as [int]) + 1
        $wsNames[$item.workspace_id] = $item.workspace_name
    }
    $wsStaleItems = @{}   # workspace_id -> stale item count
    foreach ($c in $Candidates) {
        $wsStaleItems[$c.workspace_id] = ($wsStaleItems[$c.workspace_id] -as [int]) + 1
    }

    $wsRows = ""
    $allStaleCount = 0
    foreach ($wsId in ($wsNames.Keys | Sort-Object { $wsNames[$_] })) {
        $total = $wsTotalItems[$wsId]
        $stale = if ($wsStaleItems[$wsId]) { $wsStaleItems[$wsId] } else { 0 }
        if ($stale -eq 0) { continue }  # skip workspaces with no stale items
        $allStale = ($stale -eq $total)
        if ($allStale) { $allStaleCount++ }
        $rowClass = if ($allStale) { ' class="all-stale"' } else { '' }
        $statusBadge = if ($allStale) {
            '<span class="badge stale">ALL STALE</span>'
        } else {
            '<span class="badge warn">Partial</span>'
        }
        $wsRows += @"

        <tr$rowClass>
            <td>$($wsNames[$wsId])</td>
            <td>$total</td>
            <td>$stale</td>
            <td>$($total - $stale)</td>
            <td>$statusBadge</td>
        </tr>
"@
    }

    $rowsHtml = ""
    foreach ($c in $Candidates) {
        $depsUp = $c.upstream
        $depsDown = $c.downstream

        if ($depsUp -and $depsUp.Count -gt 0) {
            $upItems = ($depsUp | ForEach-Object {
                "<li>$($_.type): $($_.name) <span class='ws'>($($_.workspace))</span></li>"
            }) -join ""
            $upHtml = "<ul>$upItems</ul>"
        }
        else { $upHtml = "<em>None</em>" }

        if ($depsDown -and $depsDown.Count -gt 0) {
            $downItems = ($depsDown | ForEach-Object {
                "<li>$($_.type): $($_.name) <span class='ws'>($($_.workspace))</span></li>"
            }) -join ""
            $downHtml = "<ul>$downItems</ul>"
        }
        else { $downHtml = "<em>None</em>" }

        $days = $c.days_since_refresh
        $badge = if ($days -is [int] -and $days -gt 60) { "stale" } else { "warn" }

        $rowsHtml += @"

        <tr>
            <td>$($c.workspace_name)</td>
            <td>$($c.item_name)</td>
            <td>$($c.item_type)</td>
            <td>$($c.last_refresh)</td>
            <td><span class="badge $badge">$days days</span></td>
            <td>$upHtml</td>
            <td>$downHtml</td>
        </tr>
"@
    }

    $totalCandidates = $Candidates.Count
    $smCount = ($Candidates | Where-Object { $_.item_type -eq "SemanticModel" }).Count
    $rptCount = ($Candidates | Where-Object { $_.item_type -eq "Report" }).Count
    $withDownstream = ($Candidates | Where-Object { $_.downstream -and $_.downstream.Count -gt 0 }).Count

    if (-not $rowsHtml) {
        $rowsHtml = '<tr><td colspan="7" style="text-align:center;padding:2rem;">No stale items found.</td></tr>'
    }

    $generated = $Now.ToString("MMMM dd, yyyy 'at' hh:mm tt")

    $html = @"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Fabric Stale Items Report</title>
<style>
    :root { --bg: #f8f9fa; --card: #fff; --border: #dee2e6; --accent: #0078d4; --danger: #d13438; --warn: #f7630c; }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: var(--bg); color: #323130; padding: 2rem; }
    .container { max-width: 1400px; margin: 0 auto; }
    header { background: var(--accent); color: #fff; padding: 1.5rem 2rem; border-radius: 8px 8px 0 0; }
    header h1 { font-size: 1.5rem; font-weight: 600; }
    header p { opacity: 0.9; margin-top: 0.25rem; font-size: 0.9rem; }
    .summary { display: flex; gap: 1.5rem; padding: 1.5rem 2rem; background: var(--card); border-bottom: 1px solid var(--border); }
    .stat { text-align: center; }
    .stat .num { font-size: 2rem; font-weight: 700; color: var(--accent); }
    .stat .label { font-size: 0.8rem; color: #605e5c; text-transform: uppercase; letter-spacing: 0.05em; }
    .section-header { background: #f3f2f1; padding: 1rem 2rem; font-size: 1.1rem; font-weight: 600; border-bottom: 1px solid var(--border); margin-top: 2rem; border-radius: 8px 8px 0 0; }
    .section-header:first-of-type { margin-top: 0; }
    tr.all-stale td { background: #fde7e9; }
    .table-wrap { background: var(--card); border-radius: 0 0 8px 8px; overflow-x: auto; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    .table-wrap + .section-header { margin-top: 2rem; }
    table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
    th { background: #f3f2f1; text-align: left; padding: 0.75rem 1rem; font-weight: 600; position: sticky; top: 0; border-bottom: 2px solid var(--border); }
    td { padding: 0.75rem 1rem; border-bottom: 1px solid var(--border); vertical-align: top; }
    tr:hover td { background: #f5f5f5; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; }
    .badge.warn { background: #fff4ce; color: #8a6d00; }
    .badge.stale { background: #fde7e9; color: var(--danger); }
    ul { list-style: none; padding: 0; }
    li { padding: 2px 0; }
    .ws { color: #605e5c; font-size: 0.8em; }
    .footer { text-align: center; padding: 1rem; color: #a19f9d; font-size: 0.8rem; }
</style>
</head>
<body>
<div class="container">
    <header>
        <h1>Fabric Stale Items Report</h1>
        <p>Generated $generated &mdash; Items not refreshed in ${StaleThresholdDays}+ days</p>
    </header>
    <div class="summary">
        <div class="stat"><div class="num">$totalCandidates</div><div class="label">Deletion Candidates</div></div>
        <div class="stat"><div class="num">$smCount</div><div class="label">Semantic Models</div></div>
        <div class="stat"><div class="num">$rptCount</div><div class="label">Reports</div></div>
        <div class="stat"><div class="num">$withDownstream</div><div class="label">With Downstream Deps</div></div>
        <div class="stat"><div class="num">$allStaleCount</div><div class="label">Fully Stale Workspaces</div></div>
    </div>
    <div class="section-header">Workspace Summary</div>
    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>Workspace</th>
                    <th>Total Items</th>
                    <th>Stale Items</th>
                    <th>Fresh Items</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                $wsRows
            </tbody>
        </table>
    </div>
    <div class="section-header">Stale Items Detail</div>
    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>Workspace</th>
                    <th>Item Name</th>
                    <th>Type</th>
                    <th>Last Refresh</th>
                    <th>Days Stale</th>
                    <th>Upstream Dependencies</th>
                    <th>Downstream Dependencies</th>
                </tr>
            </thead>
            <tbody>
                $rowsHtml
            </tbody>
        </table>
    </div>
    <div class="footer">Threshold: items with no refresh in the last $StaleThresholdDays days are flagged as deletion candidates.</div>
</div>
</body>
</html>
"@
    return $html
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
Write-Host "Authenticating ..."
$token = Get-FabricToken
$hdrs = Get-AuthHeaders -Token $token
$now = [datetime]::UtcNow
$cutoff = $now.AddDays(-$StaleThresholdDays)

# 1. List workspaces
Write-Host "Listing workspaces ..."
$workspaces = Get-FabricWorkspaces -Headers $hdrs
Write-Host "  Found $($workspaces.Count) workspaces"

# 2. Enumerate semantic models and reports per workspace
$allItems = [System.Collections.Generic.List[hashtable]]::new()
$workspaceIds = [System.Collections.Generic.List[string]]::new()

foreach ($ws in $workspaces) {
    $wsId = $ws.id
    $wsName = if ($ws.displayName) { $ws.displayName } elseif ($ws.name) { $ws.name } else { $wsId }
    $workspaceIds.Add($wsId)
    Write-Host "  Scanning workspace: $wsName"

    # Semantic models
    try {
        $models = Get-FabricSemanticModels -WorkspaceId $wsId -Headers $hdrs
    }
    catch {
        Write-Host "    Skipping semantic models ($_)"
        $models = @()
    }
    foreach ($m in $models) {
        $mName = if ($m.displayName) { $m.displayName } elseif ($m.name) { $m.name } else { $m.id }
        $allItems.Add(@{
            workspace_id   = $wsId
            workspace_name = $wsName
            item_id        = $m.id
            item_name      = $mName
            item_type      = "SemanticModel"
        })
    }

    # Reports
    try {
        $reports = Get-FabricReports -WorkspaceId $wsId -Headers $hdrs
    }
    catch {
        Write-Host "    Skipping reports ($_)"
        $reports = @()
    }
    foreach ($r in $reports) {
        $rName = if ($r.displayName) { $r.displayName } elseif ($r.name) { $r.name } else { $r.id }
        $allItems.Add(@{
            workspace_id   = $wsId
            workspace_name = $wsName
            item_id        = $r.id
            item_name      = $rName
            item_type      = "Report"
        })
    }
}

Write-Host "`nTotal items found: $($allItems.Count)"

# 3. Check refresh history for semantic models
Write-Host "Checking refresh history for semantic models ..."
$smRefresh = @{}
foreach ($item in $allItems) {
    if ($item.item_type -ne "SemanticModel") { continue }
    $refreshes = @(Get-RefreshHistory -WorkspaceId $item.workspace_id -DatasetId $item.item_id -Headers $hdrs)
    if ($refreshes -and $refreshes.Count -gt 0) {
        $endTimeStr = if ($refreshes[0].endTime) { $refreshes[0].endTime } else { $refreshes[0].startTime }
        if ($endTimeStr) {
            try {
                $endTime = [datetime]::Parse($endTimeStr, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AdjustToUniversal)
                $smRefresh[$item.item_id] = $endTime
            }
            catch {
                $smRefresh[$item.item_id] = $null
            }
        }
        else {
            $smRefresh[$item.item_id] = $null
        }
    }
    else {
        $smRefresh[$item.item_id] = $null
    }
}

# 4. Identify deletion candidates
$candidates = [System.Collections.Generic.List[hashtable]]::new()
foreach ($item in $allItems) {
    if ($item.item_type -ne "SemanticModel") { continue }
    $last = $smRefresh[$item.item_id]
    if ($null -eq $last -or $last -lt $cutoff) {
        $item["last_refresh"] = if ($last) { $last.ToString("yyyy-MM-dd HH:mm 'UTC'") } else { "Never" }
        $item["days_since_refresh"] = if ($last) { [int]($now - $last).TotalDays } else { "N/A" }
        $candidates.Add($item)
    }
}

Write-Host "`nDeletion candidates (semantic models): $($candidates.Count)"

# 5. Run scanner for lineage on workspaces that have candidates
$candidateWsIds = ($candidates | ForEach-Object { $_.workspace_id } | Sort-Object -Unique)
$scanData = @{}

if ($candidateWsIds) {
    Write-Host "Running workspace scanner for lineage ..."
    $scanData = Invoke-WorkspaceScanForLineage -WorkspaceIds $candidateWsIds -Headers $hdrs
    Write-Host "  Scanned $($scanData.Count) workspaces with lineage data"

    # Also check for stale reports via their underlying dataset
    $nameLookup = Get-NameLookup -ScanData $scanData
    foreach ($wsId in $scanData.Keys) {
        $wsScan = $scanData[$wsId]
        $wsName = if ($wsScan.name) { $wsScan.name } else { $wsId }
        foreach ($rpt in $wsScan.reports) {
            $dsId = $rpt.datasetId
            if ($dsId -and $smRefresh.ContainsKey($dsId)) {
                $last = $smRefresh[$dsId]
                if ($null -eq $last -or $last -lt $cutoff) {
                    # Avoid duplicates
                    $duplicate = $candidates | Where-Object { $_.item_id -eq $rpt.id }
                    if (-not $duplicate) {
                        $candidates.Add(@{
                            workspace_id       = $wsId
                            workspace_name     = $wsName
                            item_id            = $rpt.id
                            item_name          = if ($rpt.name) { $rpt.name } else { $rpt.id }
                            item_type          = "Report"
                            last_refresh       = if ($last) { $last.ToString("yyyy-MM-dd HH:mm 'UTC'") } else { "Never (via model)" }
                            days_since_refresh = if ($last) { [int]($now - $last).TotalDays } else { "N/A" }
                        })
                    }
                }
            }
        }
    }
}

# 6. Enrich candidates with dependency info
if ($scanData.Count -gt 0) {
    Write-Host "Resolving upstream / downstream dependencies ..."
    $nameLookup = Get-NameLookup -ScanData $scanData
    foreach ($c in $candidates) {
        $deps = Get-ItemDependencies -ScanData $scanData -ItemId $c.item_id -ItemType $c.item_type
        foreach ($d in ($deps.upstream + $deps.downstream)) {
            $resolvedName = $nameLookup[$d.name]
            if (-not $resolvedName) { $resolvedName = $nameLookup[$d.id] }
            if ($resolvedName) { $d.name = $resolvedName }
        }
        $c["upstream"] = $deps.upstream
        $c["downstream"] = $deps.downstream
    }
}

# Sort: items with downstream deps first (higher risk to delete)
$sortedCandidates = $candidates | Sort-Object @(
    @{ Expression = { -($_.downstream.Count) }; Ascending = $true },
    @{ Expression = { $_.workspace_name }; Ascending = $true },
    @{ Expression = { $_.item_name }; Ascending = $true }
)

# 7. Generate HTML report
Write-Host "`nGenerating HTML report -> $OutputFile"
$html = New-HtmlReport -Candidates @($sortedCandidates) -AllItems @($allItems) -Now $now
$html | Out-File -FilePath $OutputFile -Encoding utf8
Write-Host "Done. $($candidates.Count) candidates written to $OutputFile"
