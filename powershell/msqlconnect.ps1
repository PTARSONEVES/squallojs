#Teste de Internet
Clear
$conn = (Test-Connection google.com -Count 1 -Quiet)
if ($conn -eq "true") { Write-Host "Internet Funcionando" -ForegroundColor Yellow }

function Connect-MySQL($user, $pass, $MySQLHost, $database) {
    # Load MySQL .NET Connector Objects
    [void][system.reflection.Assembly]::LoadWithPartialName("MySql.Data")

    # Open Connection
    $connStr = "server=" + $MySQLHost + ";port=3306;uid=" + $user + ";pwd=" + $pass + ";database=" + $database + ";Pooling=FALSE"
    $conn = New-Object MySql.Data.MySqlClient.MySqlConnection($connStr)
    $conn.Open()
    return $conn
}

function Disconnect-MySQL($conn) {
    $conn.Close()
}

function Connect-MySQL1([string]$user, [string]$pass, [string]$MySQLHost, [string]$database) {
    # Load MySQL .NET Connector Objects 
    [void][system.reflection.Assembly]::LoadWithPartialName("MySql.Data")
    # Open Connection
    $connStr = "server=" + $MySQLHost + ";port=3306;uid=" + $user + ";pwd=" + $pass + ";database=" + $database + ";Pooling=FALSE"
    try {
        $conn = New-Object MySql.Data.MySqlClient.MySqlConnection($connStr)
        $conn.Open()
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

function  Execute-MySQLQuery($query) {
    # Não consulta - Inserir/Atualizar/Excluir consulta onde nenhum dado de retorno é necessário
    $cmd = New-Object MySql.Data.MySqlClient.MySqlCommand($query, $conn)          # Criar comando SQL
    $dataAdapter = New-Object MySql.Data.MySqlClient.MySqlDataAdapter($cmd)       # Criar adaptador de dados a partir do comando de consulta
    $dataSet = New-Object System.Data.DataSet                                     # Criar conjunto de dados
    $dataAdapter.Fill($dataSet, "data")                                           # Preencher conjunto de dados a partir do adaptador de dados, com o nome "data"
    $cmd.Dispose()
    return  $dataSet.Tables["data"]                                               # Retorna uma matriz de resultados
}

function Execute-MySQLScalar($query) {
    # Consulta escalar - Select etc onde um único valor de dados de retorno é esperado
    $cmd = $conn.CreateCommand()                                              # Cria objeto de comando 
    $cmd.CommandText = $query                                                 # Carrega consulta no objeto 
    $cmd.ExecuteScalar()                                                      # Executa comando 
}

function tabelas($query,$format) {
    $result = Execute-MySQLQuery($query)
    if ($format = 'tabela') {
        $result  |  Format-Table
    }
    Write-Host  ("Há " + $result.rows.count + " linha(s) nessa consulta")
}

function ocorrencias($query) {
    $cnt = Execute-MySQLScalar $query
    return $cnt
}

Clear-Host
$conn = (Test-Connection google.com -Count 1 -Quiet)
if ($conn -eq "true") { Write-Host "Internet Funcionando" -ForegroundColor Yellow }


$user = 'root'
$pass = 'Strol@ndi@1'
$MySQLHost = 'localhost'
$database = 'sped_efd'

$conn = Connect-MySQL1 $user $pass $MySQLHost $database

$reg = "'|0200|%';"

$query = "SELECT * FROM sped_txt where texto LIKE " + $reg
tabelas $query 'tabela'

$query = "SELECT registro FROM registros;"
tabelas $query 'tabela'

$query = "SELECT COUNT(texto) FROM sped_txt where texto LIKE " + $reg
$cnt = ocorrencias $query
Write-Host ("Há " + $cnt + " ocorrência(s) desse registro...")

$conn.Close()