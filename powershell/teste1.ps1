function Execute-Sql($sql) {

    $query = $sql

}

function Remove-Diacritics 
{
  param ([String]$sToModify = [String]::Empty)

  foreach ($s in $sToModify) # Param may be a string or a list of strings
  {
    if ($sToModify -eq $null) {return [string]::Empty}

    $sNormalized = $sToModify.Normalize("FormD")

    foreach ($c in [Char[]]$sNormalized)
    {
      $uCategory = [System.Globalization.CharUnicodeInfo]::GetUnicodeCategory($c)
      if ($uCategory -ne "NonSpacingMark") {$res += $c}
    }

    return $res
  }
}


$dirsql='D:\temp\sped\importacoes\sql'
Clear-Host
#Clear-Content teste.txt
$sqls = Get-Content -Path $dirsql\sql1.txt

Write-Host $sqls

foreach ($sql in $sqls) {Write-Host $sql}

exit
$fim=$from.Count
for ($i=0;$i -le $fim-1;$i++) {
    $linha=$from[$i]
    $j=$i+1
    $norm = Remove-Diacritics $linha
    Write-Host 'Linha '$j' de '$fim': ' $norm
    Add-Content -Path teste.txt -Value $norm -Encoding UTF8
}
Clear-Content $sped
Add-Content -Path $sped -Value $From

#$name = "Reencarnação aviôes avô SAÍDA"
#Write-Host (Remove-Diacritics $name)
#$test = ("äâûê", "éèà", "ùçä")
#$test | % {Remove-Diacritics $_}
#Remove-Diacritics $from
#Write-Host $from.Count