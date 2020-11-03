using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Dapper.Contrib.Extensions;
using FunctionAppCargaArquivos.Models;

namespace FunctionAppCargaArquivos
{
    public static class CargaArquivoCsvBlobTrigger
    {
        [FunctionName("CargaArquivoCsvBlobTrigger")]
        public static void Run([BlobTrigger("arquivoscsv-processamento/{name}", Connection = "AzureWebJobsStorage")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"Arquivo: {name}");

            if (!name.ToLower().EndsWith(".csv"))
            {
                log.LogError($"O arquivo {name} não será processado já que possui uma extensão inválida!");
                return;
            }

            if (myBlob.Length > 0)
            {
                using var reader = new StreamReader(myBlob);
                using var conexaoSql = new SqlConnection(
                    Environment.GetEnvironmentVariable("BaseArquivos"));

                Arquivo arquivo = new Arquivo()
                {
                    Nome = name,
                    DataCarga = DateTime.Now
                };
                conexaoSql.Insert(arquivo);
                log.LogInformation(
                    $"Id gerado para o arquivo: {arquivo.IdArquivo}");
                
                int numLinha = 1;
                string linha = reader.ReadLine();
                while (linha != null)
                {
                    conexaoSql.Insert(new LinhaArquivo
                    {
                        IdArquivo = arquivo.IdArquivo,
                        NumLinha = numLinha,
                        Conteudo = linha
                    });
                    log.LogInformation($"Linha {numLinha}: {linha}");

                    numLinha++;
                    linha = reader.ReadLine();
                }
    
                log.LogInformation($"Concluído o processamento do arquivo {name}");
            }
            else
                log.LogError($"O arquivo {name} não possui conteúdo!");
        }
    }
}