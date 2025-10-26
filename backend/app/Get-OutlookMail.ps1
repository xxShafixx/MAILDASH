Param(
  [ValidateSet("List","FetchSave")]
  [string]$Mode = "List",

  [string]$SubjectHint = "",
  [string]$Client = "",
  [string]$Region = "",
  [int]$Hours = 240,
  [int]$Limit = 50,
  [string]$SaveDir = "backend\data",

  # Exact single sender match for FetchSave (SMTP)
  [string]$Sender = "",

  # Allow-list for List mode (pass once as comma-separated string)
  [string]$AllowedSenders = ""
)


function New-JsonResult($obj) {
  $json = $obj | ConvertTo-Json -Depth 6 -Compress
  Write-Output $json
}

function Get-OutlookApp {
  try { return [Runtime.InteropServices.Marshal]::GetActiveObject("Outlook.Application") }
  catch { return New-Object -ComObject Outlook.Application }
}

function Get-InboxItems($app) {
  $ns = $app.GetNamespace("MAPI")
  $inbox = $ns.GetDefaultFolder(6) # olFolderInbox
  $items = $inbox.Items
  $items.Sort("ReceivedTime", $true) # Desc
  return $items
}

function Is-ExcelLike($att) {
  $name = ($att.FileName | ForEach-Object { $_.ToString() }).ToLower()
  return ($name -like "*.xlsx" -or $name -like "*.xls" -or $name -like "*.xlsm" -or $name -like "*.xlsb" -or $name -like "*.csv")
}

function Resolve-SenderEmail($mail) {
  try {
    $pa = $mail.PropertyAccessor
    $smtp = $pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x5D01001F")
    if ($smtp -and $smtp.Trim().Length -gt 0) { return $smtp.Trim() }
  } catch {}
  try {
    if ($mail.SenderEmailType -eq "EX" -and $mail.Sender -and $mail.Sender.GetExchangeUser()) {
      $exUser = $mail.Sender.GetExchangeUser()
      if ($exUser -and $exUser.PrimarySmtpAddress) { return $exUser.PrimarySmtpAddress.Trim() }
    }
  } catch {}
  try { if ($mail.SenderEmailAddress) { return $mail.SenderEmailAddress.Trim() } } catch {}
  return $null
}

function Sanitize-Name([string]$name) {
  if (-not $name) { return "_" }
  $s = ($name -replace '[\\/:*?"<>|]', '_').Trim().TrimEnd('.')
  if (-not $s) { $s = "_" }
  return $s
}

function Compose-SavePath([string]$base, [string]$client, [string]$region, [string]$originalFile) {
  $c = Sanitize-Name $client
  $r = if ([string]::IsNullOrWhiteSpace($region)) { "__no_region__" } else { Sanitize-Name $region }
  $ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
  $orig = if ($originalFile) { Sanitize-Name $originalFile } else { "Attachment.xlsx" }
  $dir = Join-Path $base $c
  $dir = Join-Path $dir $r
  if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  return Join-Path $dir ($ts + "__" + $orig)
}

function Is-MailItem($it) {
  try { return ($it.Class -eq 43) } catch { return $false }
}

function Get-MailIds($mail) {
  $entry = $null
  $inet  = $null
  $conv  = $null
  $searchKeyHex = $null

  try { $entry = $mail.EntryID } catch {}

  try {
    $pa = $mail.PropertyAccessor
    try { $inet = $pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x1035001F") } catch {}
    if (-not $inet) { try { $inet = $pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x1035001E") } catch {} }
    if (-not $inet) {
      try {
        $hdrs = $pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x007D001E")
        if (-not $hdrs) { $hdrs = $pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x007D001F") }
        if ($hdrs) {
          if ($hdrs -match "(?im)^[ \t]*Message-ID:\s*<([^>]+)>\s*$") {
            $inet = "<$($Matches[1])>"
          } elseif ($hdrs -match "(?im)^[ \t]*Message-ID:\s*(.+?)\s*$") {
            $inet = $Matches[1].Trim()
          }
        }
      } catch {}
    }
    try {
      $sk = $pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x300B0102")
      if ($sk) { $searchKeyHex = ([System.BitConverter]::ToString($sk) -replace '-', '').ToLower() }
    } catch {}
    try {
      $bin = $pa.GetProperty("http://schemas.microsoft.com/mapi/proptag/0x30130102")
      if ($bin) { $conv = ([System.BitConverter]::ToString($bin) -replace '-', '').ToLower() }
    } catch {}
  } catch {}

  return [PSCustomObject]@{
    entryId            = $entry
    internetMessageId  = $inet
    conversationIdHex  = $conv
    searchKeyHex       = $searchKeyHex
  }
}

# ===================== List mode =====================
if ($Mode -eq "List") {
  try {
    $app = Get-OutlookApp
    $items = Get-InboxItems -app $app
    $cutoff = (Get-Date).ToUniversalTime().AddHours(-[Math]::Max(1,$Hours))
    
    $allow = @()
    if (-not [string]::IsNullOrWhiteSpace($AllowedSenders)) {
      $AllowedSenders.Split(',') | ForEach-Object { if ($_.Trim()) { $allow += $_.Trim().ToLower() } }
    }

    $out = @()
    $count = 0
    foreach ($it in $items) {
      if ($null -eq $it -or -not (Is-MailItem $it)) { continue }

      $rcv2 = $null; try { $rcv2 = [DateTime]$it.ReceivedTime } catch {}
      if ($rcv2 -and $rcv2.ToUniversalTime() -lt $cutoff) { break }

      $fromEmail = Resolve-SenderEmail $it

      if ($allow.Count -gt 0) {
        if (-not $fromEmail -or -not ($allow -contains $fromEmail.ToLower())) { continue }
      }

      $ids = Get-MailIds $it
      $hasAtt = $false; $attNames = @()
      try {
        if ($it.Attachments.Count -gt 0) {
          $hasAtt = $true
          $it.Attachments | ForEach-Object { $attNames += $_.FileName }
        }
      } catch {}
      
      $received = $null; try { $received = (Get-Date $it.ReceivedTime).ToString("s") } catch {}

      $out += [PSCustomObject]@{
        subject            = $it.Subject
        from               = $it.SenderName
        fromEmail          = $fromEmail
        receivedDateTime   = $received
        hasAttachments     = $hasAtt
        attachments        = $attNames
        entryId            = $ids.entryId
        internetMessageId  = $ids.internetMessageId
        conversationIdHex  = $ids.conversationIdHex
        searchKeyHex       = $ids.searchKeyHex
      }
      $count++; if ($count -ge $Limit) { break }
    }
    New-JsonResult @{ ok = $true; items = $out }
  } catch {
    New-JsonResult @{ ok = $false; error = $_.Exception.Message }
  }
  exit 0
}


# ===================== FetchSave mode =====================
if ($Mode -eq "FetchSave") {
  try {
    $tokens = @()
    # 1. Always add user's specific keyword(s)
    if ($SubjectHint) {
        $SubjectHint -split '[, ]+' | Where-Object { $_ -ne "" } | ForEach-Object { $tokens += $_.ToLower() }
    }
    # 2. ALWAYS add Client and Region as required keywords for the subject search.
    if ($Client) { $tokens += $Client.ToLower() }
    if ($Region) { $tokens += $Region.ToLower() }
    
    # ---------------- Setup / cutoff ----------------
    $cutoff = (Get-Date).ToUniversalTime().AddHours(-[Math]::Max(1,$Hours))
    if (-not (Test-Path $SaveDir)) { New-Item -ItemType Directory -Force -Path $SaveDir | Out-Null }

    $app = Get-OutlookApp
    $items = Get-InboxItems -app $app

    foreach ($it in $items) {
      if ($null -eq $it -or -not (Is-MailItem $it)) { continue }

      $rcv = $null; try { $rcv = [DateTime]$it.ReceivedTime } catch {}
      if ($rcv -and $rcv.ToUniversalTime() -lt $cutoff) { break }
      
      # FILTER 1: Use exact sender email if provided. This is the most reliable filter.
      $fromEmail = Resolve-SenderEmail $it
      if ($Sender -and ($fromEmail.ToLower() -ne $Sender.ToLower())) {
          continue 
      }

      # FILTER 2: Subject must contain ALL tokens (Client, Region, and Keyword).
      $sub = ($it.Subject | ForEach-Object { $_.ToString() })
      $low_normalized = $sub.ToLower() -replace '\s+'

      $match = $true
      foreach ($t in $tokens) {
        if ($low_normalized -notlike "*$t*") {
            $match = $false
            break 
        }
      }
      if (-not $match) { continue }

      # ---------------- Attachments: only Excel/CSV ----------------
      if ($it.Attachments.Count -gt 0) {
        foreach ($a in $it.Attachments) {
          if (-not (Is-ExcelLike $a)) { continue }

          $targetPath = Compose-SavePath -base $SaveDir -client $Client -region $Region -originalFile $a.FileName
          $a.SaveAsFile($targetPath)
          $ids = Get-MailIds $it
          New-JsonResult @{
            ok                 = $true
            saved_path         = $targetPath
            mail_subject       = $sub
            from               = $it.SenderName
            fromEmail          = $fromEmail
            receivedDateTime   = $(try { (Get-Date $it.ReceivedTime).ToString("s") } catch { $null })
            entryId            = $ids.entryId
            internetMessageId  = $ids.internetMessageId
            conversationIdHex  = $ids.conversationIdHex
            searchKeyHex       = $ids.searchKeyHex
          }
          exit 0
        }
      }
    }

    New-JsonResult @{ ok = $false; reason = "No matching mail or no Excel/CSV attachment found" }
  } catch {
    New-JsonResult @{ ok = $false; reason = $_.Exception.Message }
  }
  exit 0
}
