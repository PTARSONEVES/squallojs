﻿# ============================================================================================================================================
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

 Write-Host $query

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
# Variáveis
#
# ============================================================================================================================================

$user = 'root'
$pass = 'Strol@ndi@1'
$MySQLHost = 'localhost'
$database = 'sped_efd'


$linha='linha.txt'
$sped='sped.txt'
$spedori='spedori.txt'

$dirPws='D:\temp\sped'
$dirPwsMySQL='D:/temp/sped'
$dirImp=$dirPws+'\importacoes'
$dirImpMySQL=$dirPwsMySQL+'/importacoes'
$dirSql='D:\temp\sped\importacoes\sql'
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

            $query = 'INSERT INTO spedimport (`nomearquivo`,`created_at`,`updated_at`) VALUE ("'+$arquivos[$arq].BaseName+'",NOW(),NOW());'
            Execute-MySQLNonQuery $conn $query
  #          Clear-Host
        } else {
            Write-Host 'Arquivo já processado: ' $arquivos[$arq].BaseName -ForegroundColor Green
        }
    }

   # Remove-Item -Path $dirArquivos/*.*

$conn.Close()

exit

