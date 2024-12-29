using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace nuget_v_airflow_client
{
    public interface IAirflowClient
    {
        Task<AirflowResponse<T>> TriggerDagAsync<T>(string dagId, object payload = null, CancellationToken cancellationToken = default);
        Task<AirflowResponse<T>> GetDagRunStatusAsync<T>(string dagId, string dagRunId, CancellationToken cancellationToken = default);
    }

}
