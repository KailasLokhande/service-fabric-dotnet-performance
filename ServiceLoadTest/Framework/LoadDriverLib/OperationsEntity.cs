using Microsoft.Azure.CosmosDB.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadDriverLib
{
    public class OperationsEntity : TableEntity
    {
        public string operationName { get; set; }
        public long startTime { get; set; }
        public double duration { get; set; }
        //public double executionDuration { get; set; }

        public OperationsEntity(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        public OperationsEntity() { }
    }
}
