using System.Text.Json;
using System.Text;
using nuget_v_airflow_client;

namespace AirflowClient
{
    public class AirflowClient
    {
        private readonly HttpClient _httpClient;
        private readonly AirflowConfig _config;

        public AirflowClient(AirflowConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _httpClient = new HttpClient();

            string credentials = Convert.ToBase64String(
                Encoding.UTF8.GetBytes($"{_config.Username}:{_config.Password}"));
            _httpClient.DefaultRequestHeaders.Add("Authorization", $"Basic {credentials}");
            _httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
        }

        public async Task<AirflowResponse<T>> TriggerDagAsync<T>(
            string dagId,
            object payload = null,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var url = $"{_config.BaseUrl}/api/v1/dags/{dagId}/dagRuns";
                var content = new StringContent(
                    JsonSerializer.Serialize(payload ?? new { }),
                    Encoding.UTF8,
                    "application/json"
                );

                var response = await _httpClient.PostAsync(url, content, cancellationToken);
                var responseContent = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode)
                {
                    return new AirflowResponse<T>
                    {
                        Success = true,
                        Data = JsonSerializer.Deserialize<T>(responseContent),
                        Message = "DAG triggered successfully"
                    };
                }

                return new AirflowResponse<T>
                {
                    Success = false,
                    Message = $"Failed to trigger DAG. Status: {response.StatusCode}, Message: {responseContent}"
                };
            }
            catch (Exception ex)
            {
                return new AirflowResponse<T>
                {
                    Success = false,
                    Message = $"Error triggering DAG: {ex.Message}"
                };
            }
        }

        public async Task<AirflowResponse<T>> GetDagRunStatusAsync<T>(
            string dagId,
            string dagRunId,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var url = $"{_config.BaseUrl}/api/v1/dags/{dagId}/dagRuns/{dagRunId}";
                var response = await _httpClient.GetAsync(url, cancellationToken);
                var responseContent = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode)
                {
                    return new AirflowResponse<T>
                    {
                        Success = true,
                        Data = JsonSerializer.Deserialize<T>(responseContent),
                        Message = "DAG run status retrieved successfully"
                    };
                }

                return new AirflowResponse<T>
                {
                    Success = false,
                    Message = $"Failed to get DAG run status. Status: {response.StatusCode}, Message: {responseContent}"
                };
            }
            catch (Exception ex)
            {
                return new AirflowResponse<T>
                {
                    Success = false,
                    Message = $"Error getting DAG run status: {ex.Message}"
                };
            }
        }
    }

}