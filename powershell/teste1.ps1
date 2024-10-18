function copia_tabela() {

    $tables='D:/temp/sped/importacoes/tabelas.txt'
    if (Test-Path $tables) {
        Remove-Item $tables
    }
    $query = 'USE sped_efd;'
    Execute-MySQLNonQuery $conn $query
    $query = "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE table_schema='sped_tabelas' INTO OUTFILE '$tables';"
    Execute-MySQLNonQuery $conn $query

    $tabelas = Get-Content -Path $tables

    foreach ($table in $tabelas) {
        Write-Host "Criando a tabela tbs_$table"
        $query = "DROP TABLE if EXISTS tbs_$table;"
        Execute-MySQLNonQuery $conn $query
        $query = "CREATE TABLE tbs_$table SELECT * FROM sped_tabelas.$table;"
        Execute-MySQLNonQuery $conn $query

        $tbl='tbs_'+$table
        Write-Host 'Alterando '$tbl

        $query = "ALTER TABLE $tbl CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT FIRST, ADD PRIMARY KEY (`id`);"
        Execute-MySQLNonQuery $conn $query
    }
}



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