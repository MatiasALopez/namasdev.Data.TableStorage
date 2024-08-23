using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;

namespace namasdev.Data.TableStorage
{
    public class TableRepositorio<T>
        where T : TableEntity, new()
    {
        private const int BATCH_TAMAÑO_MAXIMO = 100;

        private readonly CloudStorageAccount _account;
        private readonly string _tableName;
        private CloudTable _cloudTable;

        public TableRepositorio(CloudStorageAccount account, string tableName)
        {
            if (account == null)
            {
                throw new ArgumentNullException("account");
            }
            if (String.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentNullException("tableName");
            }

            _account = account;
            _tableName = tableName;
        }

        private CloudTable CloudTable
        {
            get { return _cloudTable ?? (_cloudTable = _account.CreateCloudTableClient().GetTableReference(_tableName)); }
        }

        public TableQuery<T> CrearTableQuery()
        {
            return CloudTable.CreateQuery<T>();
        }

        public async Task<T> ObtenerAsync(string partitionKey, string rowKey)
        {
            var result = await CloudTable.ExecuteAsync(TableOperation.Retrieve<T>(partitionKey, rowKey));
            return (T)result.Result;
        }

        public IQueryable<T> Obtener(Expression<Func<T, bool>> condicion)
        {
            return CrearTableQuery()
                .Where(condicion)
                .ToList()
                .AsQueryable();
        }

        public IQueryable<T> Obtener(string partitionKey, string condicion)
        {
            return CloudTable.ExecuteQuery(new TableQuery<T>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                    TableOperators.And,
                    condicion)))
                .ToList()
                .AsQueryable();
        }

        public async Task AgregarAsync(T entidad)
        {
            await ExecuteAsync(TableOperation.Insert(entidad));
        }

        private async Task ExecuteAsync(TableOperation op)
        {
            await CloudTable.ExecuteAsync(
                op,
                requestOptions: CrearTableRequestOptions(),
                operationContext: null);
        }

        public async Task AgregarEnBatchAsync(IEnumerable<T> entidades,
            bool actualizarSiExiste = false,
            int? tamañoBatch = null)
        {
            await AgruparEntidadesPorParticionYExecuteBatchAsync(entidades,
                accion: (op, e) =>
                {
                    if (actualizarSiExiste)
                    {
                        op.InsertOrReplace(e);
                    }
                    else
                    {
                        op.Insert(e);
                    }
                },
                tamañoBatch: tamañoBatch);
        }

        private async Task AgruparEntidadesPorParticionYExecuteBatchAsync(IEnumerable<T> entidades, Action<TableBatchOperation, ITableEntity> accion,
            bool usarETagWildcard = false,
            int? tamañoBatch = null)
        {
            var operacionesBatch = new Dictionary<string, TableBatchOperation>();

            TableBatchOperation op = null;
            foreach (var e in entidades)
            {
                if (!operacionesBatch.TryGetValue(e.PartitionKey, out op))
                {
                    op = new TableBatchOperation();
                    operacionesBatch.Add(e.PartitionKey, op);
                }

                if (usarETagWildcard)
                {
                    e.ETag = "*";
                }

                accion(op, e);
            }

            var listaOps = new List<TableBatchOperation>();

            tamañoBatch =
                tamañoBatch.HasValue
                ? Math.Min(tamañoBatch.Value, BATCH_TAMAÑO_MAXIMO)
                : BATCH_TAMAÑO_MAXIMO;

            if (op.Count <= tamañoBatch)
            {
                listaOps.Add(op);
            }
            else
            {
                int cant = op.Count;
                int cantBloque = 0;
                TableBatchOperation bloqueOp = null;
                for (int i = 0; i < cant; i++)
                {
                    if (bloqueOp == null)
                    {
                        bloqueOp = new TableBatchOperation();
                        listaOps.Add(bloqueOp);
                    }

                    bloqueOp.Add(op[i]);
                    cantBloque++;

                    if (cantBloque == tamañoBatch)
                    {
                        cantBloque = 0;
                        bloqueOp = null;
                    }
                }
            }

            var batchOptions = CrearTableRequestOptions();
            var tareas = new List<Task>();
            foreach (var itemOps in listaOps)
            {
                tareas.Add(CloudTable.ExecuteBatchAsync(
                    itemOps,
                    requestOptions: batchOptions,
                    operationContext: null));
            }

            await Task.WhenAll(tareas.ToArray());
        }

        private TableRequestOptions CrearTableRequestOptions()
        {
            return new TableRequestOptions
            {
                RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 4)
            };
        }

        public async Task AgregarOActualizarAsync(T entidad)
        {
            await ExecuteAsync(TableOperation.InsertOrReplace(entidad));
        }

        public async Task ActualizarAsync(T entidad)
        {
            await ExecuteAsync(TableOperation.Replace(entidad));
        }

        public async Task ActualizarEnBatchAsync(IEnumerable<T> entidades,
            bool usarETagWildcard = false,
            int? tamañoBatch = null)
        {
            await AgruparEntidadesPorParticionYExecuteBatchAsync(entidades,
                accion: (op, e) => op.Replace(e),
                usarETagWildcard: usarETagWildcard,
                tamañoBatch: tamañoBatch);
        }

        public async Task EliminarAsync(string partitionKey, string rowKey,
            bool usarETagWildcard = false)
        {
            var entidad = new T { PartitionKey = partitionKey, RowKey = rowKey };
            if (usarETagWildcard)
            {
                entidad.ETag = "*";
            }
            await ExecuteAsync(TableOperation.Delete(entidad));
        }

        public async Task EliminarEnBatchAsync(IEnumerable<T> entidades,
            bool usarETagWildcard = false,
            int? tamañoBatch = null)
        {
            await AgruparEntidadesPorParticionYExecuteBatchAsync(entidades,
                accion: (op, e) => op.Delete(e),
                usarETagWildcard: usarETagWildcard,
                tamañoBatch: tamañoBatch);
        }

        public async Task EliminarEnBatchAsync(Expression<Func<T, bool>> condicion)
        {
            await EliminarEnBatchAsync(Obtener(condicion));
        }
    }
}
