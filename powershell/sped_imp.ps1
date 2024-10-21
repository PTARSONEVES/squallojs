# ============================================================================================================================================
#
#                                                    Funções
#
# ============================================================================================================================================
# ============================================================================================================================================
#
#  Função => CONECTA AO BANCO DE DADOS MySQL
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
#  Função => EXECUTA CONSULTA SEM RETORNO DE RESULTADO
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
#  Função => EXCECUTA CONSULTA COM RETORNO DE RESULTADO
#
# ============================================================================================================================================

function  Execute-MySQLQuery($query) {
    # Não consulta - Inserir/Atualizar/Excluir consulta onde nenhum dado de retorno é necessário
    $cmd = New-Object MySql.Data.MySqlClient.MySqlCommand($query, $conn)          # Criar comando SQL
    $dataAdapter = New-Object MySql.Data.MySqlClient.MySqlDataAdapter($cmd)       # Criar adaptador de dados a partir do comando de consulta
    $dataSet = New-Object System.Data.DataSet                                     # Criar conjunto de dados
    $dataAdapter.Fill($dataSet, "data")                                           # Preencher conjunto de dados a partir do adaptador de dados, com o nome "data"
    $cmd.Dispose()
    return  $dataSet.Tables["data"]                                               # Retorna uma matriz de resultados
}

# ============================================================================================================================================
#
#  Função => EXECUTA CONSULTA COM RETORNO DE RESULTADO UNICO
#
# ============================================================================================================================================

function Execute-MySQLScalar($query) {
    # Consulta escalar - Select etc onde um único valor de dados de retorno é esperado
    $cmd = $conn.CreateCommand()                                              # Cria objeto de comando 
    $cmd.CommandText = $query                                                 # Carrega consulta no objeto 
    $cmd.ExecuteScalar()                                                      # Executa comando 
}

# ============================================================================================================================================
#
#  Função => EXECUTA ARQUIVO DE COMANDOS SQL
#
# ============================================================================================================================================

function Execute-SQL($SQLArq) {

    $sqls = Get-Content -Path $dirsql\$SQLArq

    foreach ($sql in $sqls) {
        Execute-MySQLNonQuery $conn $sql
    }

}

# ============================================================================================================================================
#
#  Função => PROCESSA SPED
#
# ============================================================================================================================================

function processaSped($arqv) {

    Clear-Content -Path $dirImp\$linha
    Clear-Content -Path $dirImp\$sped
    Clear-Content -Path $dirImp\$spedori
    Set-Location $dirTrata
    Remove-Item  *.* 
    Set-Location $dirImp


    $from = Get-Content -Path $arqv -Encoding UTF8
    Add-Content -Path $dirImp\$spedori -Value $from -Encoding UTF8

    Select-String -Path spedori.txt -Pattern "9999" | Out-File -FilePath linha.txt
    $a = Import-Csv -Delimiter "|" -Header texto,registro,numlinhas,D -Path "linha.txt"
    foreach($ln in $a) {
        $registro = $ln.registro
        if ($registro='9999') {
            $numlinha=$ln.numlinhas
        }
    }
    Write-Host 'Total de linhas utilizáveis => '$numlinha
    $from = Get-Content -Path spedori.txt -TotalCount $numlinha

    Add-Content -Path $sped -Value $From -Encoding UTF8

    Write-Host 'Esvaziando as tabelas de importação do sped...'
    $query = "CALL PROC_ESVAZIA_SPED_IE();"
    Execute-MySQLNonQuery $conn $query

    Write-Host 'Obtendo o arquivo sped tratado...'
    $query = "LOAD DATA INFILE 'D:/temp/sped/importacoes/sped.txt' INTO TABLE spedprov;"
    Execute-MySQLNonQuery $conn $query

    Write-Host 'Verificando colunas em excesso...'
    $query = 'INSERT INTO sped_txt (`texto`) SELECT * FROM spedprov;'
    Execute-MySQLNonQuery $conn $query

    Execute-Sql 'excesso_colunas.sql'

    Write-Host 'Segmentando o sped para importação...'
    $query = "CALL PROC_GERATXT();"
    Execute-MySQLNonQuery $conn $query

    $registros = Get-ChildItem -Path $dirTrata/*.*

    $i=0
    for ($i=0;$i -le ($registros.Count -1);$i++) {
        $registro = $registros[$i]
        carregaSped $registro

    }

}

# ============================================================================================================================================
#
#  Função => ALIMENTA AS TABELAS PROVISORIAS DO SPED (reg@@@@ie)
#
# ============================================================================================================================================

function carregaSped($registro) {
    
    
    Write-Host $registro.BaseName
    $c=$registro.BaseName.Remove(0,3)
    
    $query = 'TRUNCATE TABLE reg'+$c+'ie;'
    Execute-MySQLNonQuery $conn $query

    $query = "LOAD DATA INFILE '" + $dirTrataMySQL + "/" + $registro.Name + "' INTO TABLE reg" + $c + "ie FIELDS TERMINATED BY '|';"
    Execute-MySQLNonQuery $conn $query

    $query = "DROP TABLE if EXISTS prov;"
    Execute-MySQLNonQuery $conn $query
    $query = "CREATE TABLE prov SELECT b.id,a.registro,b.fieldnum,b.idmysql,b.fieldname,b.fieldtype,b.fieldlen,b.fielddec FROM tbs_registros a,tbs_campos b WHERE a.id=b.idregistro AND a.registro='"+$c+"';
    "
    Execute-MySQLNonQuery $conn $query

    $query = "SELECT MIN(id) FROM reg"+$c+"ie;"
    $idmin = Execute-MySQLScalar $query

    $query = "SELECT * FROM reg"+$c+"ie a INNER JOIN prov b WHERE a.REG=b.registro AND a.Id="+$idmin+";"
    $ss = Execute-MySQLQuery $query

    $fim = $ss.Count
    $totalcol = $ss[1].Table.Columns.Count

    $ini = 1
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

Function normalizaSpedie($campo,$c) {
    
    $cie='reg'+$c+'ie'

    $query = "SELECT b.idmysql FROM tbs_registros a,tbs_campos b WHERE a.id=b.idregistro AND a.registro='"+$c+"' AND b.fieldname = '"+$campo+"';"
    $mysql = Execute-MySQLScalar $query

    Write-Host 'Registro a normalizar: reg'$c'ie  coluna: '$campo' código mysql = '$mysql

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


# ============================================================================================================================================
#
# Variáveis
#
# ============================================================================================================================================

$maquina = 'NOTEPTNA'

$user = 'root'
$MySQLHost = 'localhost'
$database = 'sped_efd'

if ($maquina -eq 'NOTEPTNA') {
    $pass = 'strolandia'
    $drive = 'F:'
} else {
    $pass = 'Strol@ndi@1'
    $drive = 'D:'
}


<# Database Localweb

$user = 'sped_efd'
$pass = 'Strol!ndi!1'
$MySQLHost = 'sped_efd.mysql.dbaas.com.br'
$database = 'sped_efd'

#>

$linha='linha.txt'
$sped='sped.txt'
$spedori='spedori.txt'

$dirPws=$drive+'\temp\sped'
$dirPwsMySQL=$drive+'/temp/sped'
$dirImp=$dirPws+'\importacoes'
$dirImpMySQL=$dirPwsMySQL+'/importacoes'
$dirSql=$drive+'\temp\sped\importacoes\sql'
$dirArquivos=$dirImp+'\arquivos'
$dirTrata=$dirImp+'\tratamento'
$dirTrataMySQL=$dirImpMySQL+'/tratamento'


# ============================================================================================================================================
#
# Verifica pastas
#
# ============================================================================================================================================

if (Test-Path $dirPws) {
    Set-Location $dirPws
    if (Test-Path $dirImp) {
        Set-Location $dirImp
        if (Test-Path $dirTrata) {
            Set-Location $dirTrata
            Remove-Item *.*
            Set-Location $dirImp
        } else {
            New-Item -Path $dirTrata -ItemType Directory
        }
        if (Test-Path $dirArquivos) {
            if ((Get-ChildItem -Path $dirArquivos/*.*).Count -eq 0) {
                Write-Host 'Não há arquivo para importar' -ForegroundColor Cyan
                exit
            }
        } else {
            New-Item -Path $dirArquivos -ItemType Directory
        }
        if (Test-Path $dirSql) {

        } else {
            New-Item -Path $dirSql -ItemType Directory
        }
    } else {
        New-Item -Path $dirArquivos -ItemType Directory
        New-Item -Path $dirImp -ItemType Directory
        New-Item -Path $dirTrata -ItemType Directory
        New-Item -Path $dirSql -ItemType Directory
        New-Item -Path $dirImp\$spedori -ItemType file
        New-Item -path $dirImp\$sped -ItemType file

    }
} else {
    New-Item -Path $dirPws -ItemType Directory
    New-Item -Path $dirImp -ItemType Directory
    New-Item -Path $dirTrata -ItemType Directory
    New-Item -Path $dirArquivos -ItemType Directory
}
Set-Location $dirImp

# ============================================================================================================================================
#
# Verifica Arquivos
#
# ============================================================================================================================================

if (Test-Path $linha) {
    Clear-Content $linha
} else {
    New-Item -Path $linha -ItemType file
}
if (Test-Path $sped) {
    Clear-Content $sped
} else {
    New-Item -Path $sped -ItemType file
}

# ============================================================================================================================================
#
# Processamento
#
# ============================================================================================================================================


Clear-Host

$conn = Connect-MySQL $user $pass $MySQLHost $database

$conn.Open()




    $arquivos = (Get-ChildItem -Path $dirArquivos/*.*)

    if ($arquivos.Length -gt 0) {
        $arqini=0
        if ($arquivos.Length -eq $arquivos[0].Length) {
            $arqfim = 1
        } else {
            $arqfim = $arquivos.Count
        }
    }

    for ($arq=$arqini;$arq -le $arqfim-1;$arq++) {
        $arquivo = 0
        $query = "SELECT * FROM spedimport WHERE nomearquivo='"+$arquivos[$arq].BaseName+"';"
        $arquivo = Execute-MySQLScalar $query
        $arquivo = $arquivo + 0
        if ($arquivo -eq 0) {
            Write-Host $arquivos[$arq].FullName
            processaSped $arquivos[$arq].FullName
            $codver = Execute-MySQLScalar 'SELECT COD_VER FROM reg0000ie';
            $codfin = Execute-MySQLScalar 'SELECT COD_FIN FROM reg0000ie';
            $dtini = Execute-MySQLScalar 'SELECT DT_INI FROM reg0000ie';
            $dtfim = Execute-MySQLScalar 'SELECT DT_FIM FROM reg0000ie';
            $cnpj = Execute-MySQLScalar 'SELECT CNPJ FROM reg0000ie';
            $query = 'INSERT INTO spedimport (`nomearquivo`,`nomearquivotratado`,`COD_VER`,`COD_FIN`,`DT_INI`,`DT_FIM`,`CNPJ`,`created_at`,`updated_at`) VALUE ('+"'"+$arquivos[$arq].BaseName+"','','"+$codver+"','"+$codfin+"','"+$dtini+"','"+$dtfim+"','"+$cnpj+"',NOW(),NOW());"
            Execute-MySQLNonQuery $conn $query
            $query = "SELECT id FROM spedimport WHERE nomearquivo = '"+$arquivos[$arq].BaseName+"';"
            $id = Execute-MySQLScalar $query
            $idd = "sped_"+$id+"_"+$cnpj+".txt"
            Write-Host $idd
            $query = "UPDATE spedimport SET nomearquivotratado='sped_"+$id+"_"+$cnpj+"' WHERE id="+$id+";"
            Execute-MySQLNonQuery $conn $query
            $registros = Get-ChildItem -Path $dirTrata/*.*
            New-Item -Path $dirTrata\$idd -ItemType file
            $sqls =  Get-Content -Path $dirSql\gera_sped_tratado.sql
            $fim = $registros.Count
            $ini = 0
            for ($i = $ini;$i -le $fim;$i++) {
                $local = $dirTrataMySQL+"/reg"+$registros[$i].BaseName.Remove(0,3)+"trat.txt"
                $registro = "reg"+$registros[$i].BaseName.Remove(0,3)+"ie"
                $arqori = "reg"+$registros[$i].BaseName.Remove(0,3)+"trat.txt"
                foreach ($sql in $sqls) {
                    $sql = $sql -replace 'registro', $registro
                    $sql = $sql -replace 'local', $local
                    $sql = $sql -replace 'idarquivo', $id

                    Write-Host $sql
                    Execute-MySQLNonQuery $conn $sql
                }
                Get-Content -Path $dirTrata\$arqori | Add-Content -Path $dirTrata\$idd

                #carregaSped $registro
                Write-Host $registro
            } 
 
 <#
     $sqls =  Get-Content -Path $dirSql\normaliza_decimal.sql
    foreach ($sql in $sqls) {
        $sql = $sql -replace 'arquivo', $cie
        $sql = $sql -replace 'campo', $campo
        Write-Host $sql
        Execute-MySQLNonQuery $conn $sql
    }

 #>
  #          Clear-Host
        } else {
            Write-Host 'Arquivo já processado: ' $arquivos[$arq].BaseName -ForegroundColor Green
        }
    }

   # Remove-Item -Path $dirArquivos/*.*



$conn.Close()

exit

