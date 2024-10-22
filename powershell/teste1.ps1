# ============================================================================================================================================
#
#  FunÃ§Ã£o => CONECTA AO BANCO DE DADOS MySQL
#
# ============================================================================================================================================

function Connect-MySQL([string]$user, [string]$pass, [string]$MySQLHost, [string]$database) {
    # Load MySQL .NET Connector Objects 
    [void][system.reflection.Assembly]::LoadWithPartialName("MySql.Data")
    # Open Connection
    $connStr = "server=" + $MySQLHost + ";port=3306;uid=" + $user + ";pwd=" + $pass + ";database=" + $database + ";Pooling=FALSE"
    try {
        $conn = New-Object MySql.Data.MySqlClient.MySqlConnection($connStr)
    }
    catch [System.Management.Automation.PSArgumentException] {
        Write-Host "Unable to connect to MySQL server, do you have the MySQL connector installed..?" -ForegroundColor Red
        Write-Host $_
        Exit
    }
    catch {
        Write-Host "Unable to connect to MySQL server..." -ForegroundColor Red
        Write-Host $_.Exception.GetType().FullName -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        exit
    }
    Write-Host "Conectado ao MySQL database em $MySQLHost\$database" -ForegroundColor Yellow

    return $conn
}

# ============================================================================================================================================
#
#  FunÃ§Ã£o => EXECUTA CONSULTA SEM RETORNO DE RESULTADO
#
# ============================================================================================================================================

function  Execute-MySQLNonQuery ($conn, $query) {
    $command = $conn.CreateCommand()                   # Cria objeto de comando
    $command.CommandText = $query                      # Carrega consulta no objeto
    $RowsInserted = $command.ExecuteNonQuery()         # Executa comando
    $command.Dispose()                                 # Descarta objeto de comando
    if ($RowsInserted) {
        return  $RowInserted
    }
    else {
        return  $false
    }
}

# ============================================================================================================================================
#
#  FunÃ§Ã£o => EXCECUTA CONSULTA COM RETORNO DE RESULTADO
#
# ============================================================================================================================================

function  Execute-MySQLQuery($query) {
    # NÃ£o consulta - Inserir/Atualizar/Excluir consulta onde nenhum dado de retorno Ã© necessÃ¡rio
    $cmd = New-Object MySql.Data.MySqlClient.MySqlCommand($query, $conn)          # Criar comando SQL
    $dataAdapter = New-Object MySql.Data.MySqlClient.MySqlDataAdapter($cmd)       # Criar adaptador de dados a partir do comando de consulta
    $dataSet = New-Object System.Data.DataSet                                     # Criar conjunto de dados
    $dataAdapter.Fill($dataSet, "data")                                           # Preencher conjunto de dados a partir do adaptador de dados, com o nome "data"
    $cmd.Dispose()
    return  $dataSet.Tables["data"]                                               # Retorna uma matriz de resultados
}

# ============================================================================================================================================
#
#  FunÃ§Ã£o => EXECUTA CONSULTA COM RETORNO DE RESULTADO UNICO
#
# ============================================================================================================================================

function Execute-MySQLScalar($query) {
    # Consulta escalar - Select etc onde um Ãºnico valor de dados de retorno Ã© esperado
    $cmd = $conn.CreateCommand()                                              # Cria objeto de comando 
    $cmd.CommandText = $query                                                 # Carrega consulta no objeto 
    $cmd.ExecuteScalar()                                                      # Executa comando 
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

function carregaSped($registro) {
    
    
    #Write-Host $registro
    #$c=$registro.BaseName.Remove(0,3)
    
    $query = 'TRUNCATE TABLE reg'+$registro+'ie;'
    Write-Host $query
    Execute-MySQLNonQuery $conn $query

    $query = "LOAD DATA INFILE '" + $dirTrataMySQL + "/efd" + $registro+ ".txt' INTO TABLE reg" + $registro + "ie FIELDS TERMINATED BY '|';"
    Write-Host $query
    Execute-MySQLNonQuery $conn $query

    $query = "DROP TABLE if EXISTS prov;"
    Execute-MySQLNonQuery $conn $query
    $query = "CREATE TABLE prov SELECT b.id,a.registro,b.fieldnum,b.idmysql,b.fieldname,b.fieldtype,b.fieldlen,b.fielddec FROM tbs_registros a,tbs_campos b WHERE a.id=b.idregistro AND a.registro='"+$registro+"'; "
    Write-Host $query
    Execute-MySQLNonQuery $conn $query

    $query = "SELECT MIN(id) FROM reg"+$registro+"ie;"
  Write-Host $query
    $idmin = Execute-MySQLScalar $query

    $query = "SELECT * FROM reg"+$registro+"ie a INNER JOIN prov b WHERE a.REG=b.registro AND a.Id="+$idmin+";"
  Write-Host $query
    $ss = Execute-MySQLQuery $query

    $fim = $ss.Count
    $totalcol = $ss[1].Table.Columns.Count
    Write-Host 'Total de colunas = '$totalcol

    $ini = 1
    exit
    for ($i=$ini;$i -le ($fim-1);$i++) {
        $colname = $ss[$i].Table.Columns[$i].ColumnName
        $colvalue = $ss[$i].$colname
        $mysql = $ss[$i].idmysql
        normalizaSpedie $colname $c
    }

}


# ============================================================================================================================================
#
#  Função => NORMALIZA CAMPOS SPED_IE
#
# ============================================================================================================================================

Function normalizaSpedie($campo,$registro) {
    
    $cie='reg'+$registro+'ie'

    $query = "SELECT b.idmysql FROM tbs_registros a,tbs_campos b WHERE a.id=b.idregistro AND a.registro='"+$registro+"' AND b.fieldname = '"+$campo+"';"
    $mysql = Execute-MySQLScalar $query

    Write-Host 'Registro a normalizar: reg'$registro'ie  coluna: '$campo' código mysql = '$mysql

    switch($mysql) {
        
        18 {
                if ($campo.Contains(“DT_”)) {
                    Write-Host 'Normalizando data'
                    normalizaData $campo $cie
                } else {
                    Write-Host 'Analisando...'
                    normalizaDecimal $campo $cie
                }
                break
           }       
        20 {
                if ($campo.Contains(“DT_”)) {
                    Write-Host 'Normalizando data'
                    normalizaData $campo $cie
                } else {
                    Write-Host 'Analisando...'
                    normalizaDecimal $campo $cie
                }
                break
           }       
        22 {
                if ($campo.Contains(“DT_”)) {
                    Write-Host 'Normalizando data'
                    normalizaData $campo $cie
                } else {
                    Write-Host 'Analisando...'
                    normalizaDecimal $campo $cie
                }
                break
           }       
        default {break}
    }
}

# ============================================================================================================================================
#
#  Função => NORMALIZA CAMPO DECIMAL
#
# ============================================================================================================================================

Function normalizaDecimal($campo,$cie) {
    
    $sqls =  Get-Content -Path $dirSql\normaliza_decimal.sql
    foreach ($sql in $sqls) {
        $sql = $sql -replace 'arquivo', $cie
        $sql = $sql -replace 'campo', $campo
        Write-Host $sql
        Execute-MySQLNonQuery $conn $sql
    }
    
 }

# ============================================================================================================================================
#
#  Função => NORMALIZA CAMPO DE DATA
#
# ============================================================================================================================================

Function normalizaData($campo,$cie) {
    
    $sqls =  Get-Content -Path $dirSql\normaliza_data.sql
    foreach ($sql in $sqls) {
        $sql = $sql -replace 'arquivo', $cie
        $sql = $sql -replace 'campo', $campo
        Write-Host $sql
        Execute-MySQLNonQuery $conn $sql
    }
    
 }


$maquina = 'NOTEPTNA'

$user = 'root'
if ($maquina -eq 'NOTEPTNA') {
    $pass = 'strolandia'
} else {
    $pass = 'Strol@ndi@1'
}
$MySQLHost = 'localhost'
$database = 'sped_efd'


$linha='linha.txt'
$sped='sped.txt'
$spedori='spedori.txt'

$drive='F:'
$dirPws=$drive+'\temp\sped'
$dirPwsMySQL=$drive+'/temp/sped'
$dirImp=$dirPws+'\importacoes'
$dirImpMySQL=$dirPwsMySQL+'/importacoes'
$dirSql=$drive+'\temp\sped\importacoes\sql'
$dirArquivos=$dirImp+'\arquivos'
$dirTrata=$dirImp+'\tratamento'
$dirTrataMySQL=$dirImpMySQL+'/tratamento'

Clear-Host

$conn = Connect-MySQL $user $pass $MySQLHost $database

$conn.Open()

$query = "SELECT * FROM sped_txt;"
$nreg = Execute-MySQLQuery $query
if ($nreg.Count -ge 1) {
    Remove-Item -Path $dirTrata\*.*
    $query = "SELECT * FROM tbs_registros;"
    $registros = Execute-MySQLQuery $query
    for ($i=1;$i -le ($registros.Count -1);$i++) {
        $query = "SELECT * FROM sped_txt WHERE texto LIKE '|"+$registros[$i].registro+"%';"
        $registro = Execute-MySQLQuery $query
        if ($registro.Count -ge 2) { 
           $query = "TRUNCATE TABLE spedprov;"
           Execute-MySQLNonQuery $conn $query
           $query = "REPLACE spedprov SELECT CONCAT(id,texto)  FROM sped_txt WHERE texto LIKE '|"+$registros[$i].registro+"%';" 
           Execute-MySQLNonQuery $conn $query
           $query = "SELECT texto FROM spedprov INTO OUTFILE '"+$dirTrataMySQL+"/efd"+$registros[$i].registro+".txt';" 
           Execute-MySQLNonQuery $conn $query
           Write-Host 'Criado o arquivo efd'$registros[$i].registro'.txt'
           carregaSped $registros[$i].registro
           exit
        }
    }
}



$conn.Close()