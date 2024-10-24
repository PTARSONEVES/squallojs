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

#function Connect-MySQL([string]$user, [string]$pass, [string]$MySQLHost, [string]$database) {
function Connect-MySQL($MySQLUser, $MySQLPassword, $MySQLHost, $MySQLDatabase) {

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
#        return  $false
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
#   Função => Verifica pastas
#
# ============================================================================================================================================

function verifica_pastas() {


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
                    Clear-Host
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
            if (Test-Path -Path $dirArqTrata) {
            } else {
                New-Item -Path $dirArqTrata -ItemType Directory
            }
        } else {
            New-Item -Path $dirArquivos -ItemType Directory
            New-Item -Path $dirImp -ItemType Directory
            New-Item -Path $dirArqTrata -ItemType Directory
            New-Item -Path $dirTrata -ItemType Directory
            New-Item -Path $dirSql -ItemType Directory
        }
    } else {
        New-Item -Path $dirPws -ItemType Directory
        New-Item -Path $dirImp -ItemType Directory
        New-Item -Path $dirTrata -ItemType Directory
        New-Item -Path $dirArquivos -ItemType Directory
        New-Item -Path $dirArqTrata -ItemType Directory
        New-Item -Path $dirSql -ItemType Directory
    }
    Set-Location $dirImp
}


# ============================================================================================================================================
#
#   Função => Verifica Arquivos
#
# ============================================================================================================================================

function verifica_arquivos() {

    if (Test-Path $dirImp\$linha) {
        Clear-Content $dirImp\$linha
    } else {
        New-Item -Path $dirImp\$linha -ItemType file
    }
    if (Test-Path $dirImp\$sped) {
        Clear-Content $dirImp\$sped
    } else {
        New-Item -Path $dirImp\$sped -ItemType file
    }
    if (Test-Path $dirImp\$spedTrata) {
        Clear-Content $dirImp\$spedTrata
    } else {
        New-Item -Path $dirImp\$spedTrata -ItemType file
    }
    if (Test-Path $dirImp\$spedOri) {
        Clear-Content $dirImp\$spedOri
    } else {
        New-Item -Path $dirImp\$spedOri -ItemType file
    }

}


# ============================================================================================================================================
#
#  Função => PROCESSA SPED
#
# ============================================================================================================================================

function processaSped($arqv) {

    #
    # Preparando pastas e arquivos para o processamento
    #
    Clear-Content -Path $dirImp\$linha
    Clear-Content -Path $dirImp\$sped
    Clear-Content -Path $dirImp\$spedOri
    Set-Location $dirTrata
    Remove-Item  *.* 
    Set-Location $dirImp
    #
    # Obtendo o conteudo do arquivo sped para análise (spedori.txt)
    #
    $from = Get-Content -Path $arqv -Encoding UTF8
    Add-Content -Path $dirImp\$spedOri -Value $from -Encoding UTF8
    #
    # Obtendo o número de linhas efetivamente processáveis
    #
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
    #
    # Transferindo as linhas processáveis para o arquivo sped.txt
    #
    Add-Content -Path $sped -Value $From -Encoding UTF8
    #
    # Esvaziando as tabelas reg@@@@ie
    #
    Write-Host 'Esvaziando as tabelas de importação do sped...'
    $query = "CALL PROC_ESVAZIA_SPED_IE();"
    Execute-MySQLNonQuery $conn $query
    #
    # Obtendo os dados do arquivo sped.txt na tabela sped_txt do banco de dados
    #
    Write-Host 'Obtendo o arquivo sped tratado...'
    $query = "LOAD DATA INFILE 'D:/temp/sped/importacoes/sped.txt' INTO TABLE spedprov;"
    Execute-MySQLNonQuery $conn $query
    #
    # Normalizando as colunas do arquivo em função de cada registro. Para isso será executado
    # o arquivo .SQL excesso_colunas.sql
    #
    Write-Host 'Verificando colunas em excesso...'
    $query = 'INSERT INTO sped_txt (`texto`) SELECT * FROM spedprov;'
    Execute-MySQLNonQuery $conn $query      
    Execute-Sql 'excesso_colunas.sql'
    #
    # Cria o arquivo tratado
    #
    sped_tratado

}


# ============================================================================================================================================
#
#  Função => OBTÉM O ARQUIVO SPED TRATADO
#
# ============================================================================================================================================


function sped_tratado() {

    #
    # Obtem o número total de linhas da tabela sped_txt
    #
    $query = "SELECT * FROM sped_txt;"
    $nreg = Execute-MySQLQuery $query
    if ($nreg.Count -ge 1) {
        Clear-Content -Path $dirImp\$spedOri       # limpa o conteudo do arquivo spedori.txt
        Clear-Content -Path $dirImp\$spedTrata     # limpa o conteudo do arquivo spedtrata.txt
        Remove-Item -Path $dirTrata\*.*            # limpa a pasta /sped/importacoes/tratamento
        #
        # Prepara iteração sobre os possíveis registros do SPED
        #
        $query = "SELECT * FROM tbs_registros;"
        $registros = Execute-MySQLQuery $query
        #
        # Iteração 
        #
        for ($i=1;$i -le ($registros.Count -1);$i++) {
            $query = "SELECT * FROM sped_txt WHERE texto LIKE '|"+$registros[$i].registro+"%';"
            $registro = Execute-MySQLQuery $query
            if ($registro.Count -ge 2) {                                      # Inicia o tratamento do registro específico encontrado
               Write-Host 'Processando registro ['$registros[$i].registro']' 
               $query = "TRUNCATE TABLE spedprov;"
               Execute-MySQLNonQuery $conn $query
               $query = "REPLACE spedprov SELECT CONCAT(id,texto) FROM sped_txt WHERE texto LIKE '|"+$registros[$i].registro+"%';" 
               Execute-MySQLNonQuery $conn $query
               if (Test-Path $dirTrata\spedprov.txt) {
                    Remove-Item -Path $dirTrata\spedprov.txt
               }
               $query = "SELECT texto FROM spedprov INTO OUTFILE '"+$dirTrataMySQL+"/spedprov.txt';"     # cria o arquivo spedprov.txt com todas as ocorrencias de um registo                              
               Execute-MySQLNonQuery $conn $query
               #
               # Carrega os registros em tabela do banco de dados para tratá-los
               #
               carregaSped $registros[$i].registro
            }
        }
    }

}


# ============================================================================================================================================
#
#  Função => ALIMENTA AS TABELAS PROVISORIAS DO SPED (reg@@@@ie)
#
# ============================================================================================================================================

function carregaSped($registro) {
    
    $query = 'TRUNCATE TABLE reg'+$registro+'ie;'
    Execute-MySQLNonQuery $conn $query

    $query = "LOAD DATA INFILE '" + $dirTrataMySQL + "/spedprov.txt' INTO TABLE reg" + $registro + "ie FIELDS TERMINATED BY '|';"
    Execute-MySQLNonQuery $conn $query

    $query = "DROP TABLE if EXISTS prov;"
    Execute-MySQLNonQuery $conn $query
    $query = "CREATE TABLE prov SELECT b.id,a.registro,b.fieldnum,b.idmysql,b.fieldname,b.fieldtype,b.fieldlen,b.fielddec FROM tbs_registros a,tbs_campos b WHERE a.id=b.idregistro AND a.registro='"+$registro+"'; "
    Execute-MySQLNonQuery $conn $query

    $query = "SELECT MIN(id) FROM reg"+$registro+"ie;"
    $idmin = Execute-MySQLScalar $query

    $query = "SELECT * FROM reg"+$registro+"ie a INNER JOIN prov b WHERE a.REG=b.registro AND a.Id="+$idmin+";"
    $ss = Execute-MySQLQuery $query
 
    $fim = $ss.Count
    $totalcol = $ss[1].Table.Columns.Count

    $ini = 1
    $posmysql=$fim+3
    for ($i=$ini;$i -le ($fim-1);$i++) {
        $colname = $ss[$i].Table.Columns[$i].ColumnName
        $colvalue = $ss[$i].$colname
        $mysql=$ss[$i].idmysql
        normalizaSpedie $colname $registro
    }
    Remove-Item $dirTrata\spedprov.txt
    $query = "SELECT * FROM reg"+$registro+"ie INTO OUTFILE '"+$dirTrataMySQL+"/spedprov.txt' FIELDS TERMINATED BY '|';"
    Execute-MySQLNonQuery $conn $query
    Get-Content -Path $dirTrata\spedprov.txt | Add-Content -Path $dirImp\$spedTrata
    Remove-Item $dirTrata\spedprov.txt    

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

    switch($mysql) {
        
        18 {
                if ($campo.Contains(“DT_”)) {
                    normalizaData $campo $cie
                } else {
                    normalizaDecimal $campo $cie
                }
                break
           }       
        20 {
                if ($campo.Contains(“DT_”)) {
                    normalizaData $campo $cie
                } else {
                    normalizaDecimal $campo $cie
                }
                break
           }       
        22 {
                if ($campo.Contains(“DT_”)) {
                    normalizaData $campo $cie
                } else {
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
        Execute-MySQLNonQuery $conn $sql
    }

 }


# ============================================================================================================================================
#
# Variáveis => As variáveis de ambiente são definidas em amb_var.ps1'
#
# ============================================================================================================================================

$maquina = 'NOTEPTNA'

    switch($maquina) {
        'NOTESAL'  {$PWSDrive = 'D:'}
        'NOTEPTNA' {$PWSDrive = 'F:'}
        default    {$PWSDrive = 'C:'}
    }

$linha='linha.txt'
$sped='sped.txt'
$spedOri='spedori.txt'
$spedTrata='spedtrata.txt'
$spedOri='spedori.txt'
$spedTrata='spedtrata.txt'

$dirPws=$PWSDrive+'\temp\sped'
$dirPwsMySQL=$PWSDrive+'/temp/sped'
$dirImp=$dirPws+'\importacoes'
$dirImpMySQL=$dirPwsMySQL+'/importacoes'
$dirSql=$PWSDrive+'\temp\sped\importacoes\sql'
$dirArquivos=$dirImp+'\arquivos'
$dirTrata=$dirImp+'\tratamento'
$dirTrataMySQL=$dirImpMySQL+'/tratamento'
$dirArqTrata=$dirImp+'\arqtrata'



# ============================================================================================================================================
#
# Processamento
#
# ============================================================================================================================================



verifica_pastas
verifica_arquivos

Clear-Host

$conn = Connect-MySQL $user $pass $MySQLHost $database

$conn.Open()

    #
    # Obtém quantos arquivos sped a serem tratados
    #
    $arquivos = (Get-ChildItem -Path $dirArquivos/*.*)

    if ($arquivos.Length -gt 0) {
        $arqini=0
        if ($arquivos.Length -eq $arquivos[0].Length) {
            $arqfim = 1
        } else {
            $arqfim = $arquivos.Count
        }
    }
    #
    # Iteração dos arquivos sped a serem tratados
    #
    for ($arq=$arqini;$arq -le $arqfim-1;$arq++) {
        $arquivo = 0
        $query = "SELECT * FROM spedimport WHERE nomearquivo='"+$arquivos[$arq].BaseName+"';"
        $arquivo = Execute-MySQLScalar $query
        $arquivo = $arquivo + 0
        if ($arquivo -eq 0) {
            Write-Host 'Processando o arquivo '$arquivos[$arq].FullName
            processaSped $arquivos[$arq].FullName
            #
            # Registra o arquivo sped tratado
            #
            $codver = Execute-MySQLScalar 'SELECT COD_VER FROM reg0000ie';
            $codfin = Execute-MySQLScalar 'SELECT COD_FIN FROM reg0000ie';
            $dtini = Execute-MySQLScalar 'SELECT DT_INI FROM reg0000ie';
            $dtfim = Execute-MySQLScalar 'SELECT DT_FIM FROM reg0000ie';
            $cnpj = Execute-MySQLScalar 'SELECT CNPJ FROM reg0000ie';
            $nometrata = 'SPED_TRATA_'+$cnpj+'_'+$dtini+'_'+$dtfim+'_'+$codver+'_'+$codfin
            $nomearq = 'SPED_TRATA_'+$cnpj+'_'+$dtini+'_'+$dtfim+'_'+$codver+'_'+$codfin+'.txt'
            if (Test-Path $dirArqtrata\$nomearq) {
                Remove-Item -Path $dirArqTrata\$nomearq
            }
            New-Item -Path $dirArqTrata\$nomearq -ItemType file
            Get-Content -Path $dirImp\spedtrata.txt | Add-Content -Path $dirArqTrata\$nomearq
            $query = 'INSERT INTO spedimport (`nomearquivo`,`nomearquivotratado`,`COD_VER`,`COD_FIN`,`DT_INI`,`DT_FIM`,`CNPJ`,`created_at`,`updated_at`) VALUE ('+"'"+$arquivos[$arq].BaseName+"','"+$nometrata+"','"+$codver+"','"+$codfin+"','"+$dtini+"','"+$dtfim+"','"+$cnpj+"',NOW(),NOW());"
            Execute-MySQLNonQuery $conn $query
            Clear-Host
        } else {
            Write-Host 'Arquivo já processado: ' $arquivos[$arq].BaseName -ForegroundColor Green
        }
        Remove-Item -Path $arquivos[$arq]
    }


$conn.Close()

exit

