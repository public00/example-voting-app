using System;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using Serilog;
using Serilog.Formatting.Json;
using StackExchange.Redis;

// OpenTelemetry
using OpenTelemetry;
using OpenTelemetry.Trace;
using OpenTelemetry.Resources;
using OpenTelemetry.Exporter;
using OpenTelemetry.Instrumentation.Http;

namespace Worker
{
    public class Program
    {
        private static readonly ActivitySource ActivitySource =
            new ActivitySource("Worker.ProcessVote");

        private sealed class VotePayload
        {
            public string vote { get; set; }
            public string voter_id { get; set; }
            public string traceparent { get; set; }
        }

        public static int Main(string[] args)
        {
            var endpointUrl =
                Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT")
                ?? "http://dynatrace-otel-collector:4318";

            var apiToken = Environment.GetEnvironmentVariable("DT_API_TOKEN");

            using var tracerProvider =
                Sdk.CreateTracerProviderBuilder()
                    .AddSource("Worker.ProcessVote")
                    .SetResourceBuilder(
                        ResourceBuilder.CreateDefault()
                            .AddService("worker-service"))
                    .AddHttpClientInstrumentation()
                    .AddOtlpExporter(opt =>
                    {
                        opt.Endpoint = new Uri(endpointUrl);
                        opt.Protocol = OtlpExportProtocol.HttpProtobuf;

                        if (!string.IsNullOrEmpty(apiToken))
                        {
                            opt.Headers = $"Api-Token={apiToken}";
                        }
                    })
                    .Build();

            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(new JsonFormatter())
                .CreateLogger();

            try
            {
                var pgsql = OpenDbConnection(
                    "Server=db;Username=postgres;Password=postgres;");
                var redisConn = OpenRedisConnection("redis");
                var redis = redisConn.GetDatabase();

                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                while (true)
                {
                    Thread.Sleep(100);

                    string json = redis.ListLeftPop("votes");
                    if (json == null)
                    {
                        keepAliveCommand.ExecuteNonQuery();
                        continue;
                    }

                    var payload =
                        JsonConvert.DeserializeObject<VotePayload>(json);

                    ActivityContext parentContext = default;

                    if (!string.IsNullOrEmpty(payload.traceparent)
                        && ActivityContext.TryParse(
                            payload.traceparent,
                            null,
                            out var parsedContext))
                    {
                        parentContext = parsedContext;
                    }

                    using var activity =
                        ActivitySource.StartActivity(
                            "ProcessVoteFromQueue",
                            ActivityKind.Consumer,
                            parentContext);

                    Log.ForContext("traceId", activity?.TraceId.ToString())
                       .ForContext("spanId", activity?.SpanId.ToString())
                       .Information(
                           "Processing vote {Vote} from voter {Voter}",
                           payload.vote,
                           payload.voter_id);

                    using var sqlActivity =
                        ActivitySource.StartActivity("PostgreSQL.UpdateVote");

                    UpdateVote(pgsql, payload.voter_id, payload.vote);
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
                return 1;
            }
        }

        private static NpgsqlConnection OpenDbConnection(string cs)
        {
            while (true)
            {
                try
                {
                    var conn = new NpgsqlConnection(cs);
                    conn.Open();

                    var cmd = conn.CreateCommand();
                    cmd.CommandText =
                        @"CREATE TABLE IF NOT EXISTS votes (
                            id VARCHAR(255) NOT NULL UNIQUE,
                            vote VARCHAR(255) NOT NULL
                          )";
                    cmd.ExecuteNonQuery();

                    return conn;
                }
                catch
                {
                    Thread.Sleep(1000);
                }
            }
        }

        private static ConnectionMultiplexer OpenRedisConnection(string host)
        {
            var ip =
                Dns.GetHostEntry(host)
                   .AddressList
                   .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                   .ToString();

            while (true)
            {
                try
                {
                    return ConnectionMultiplexer.Connect(ip);
                }
                catch
                {
                    Thread.Sleep(1000);
                }
            }
        }

        private static void UpdateVote(
            NpgsqlConnection conn,
            string voterId,
            string vote)
        {
            using var cmd = conn.CreateCommand();

            try
            {
                cmd.CommandText =
                    "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                cmd.Parameters.AddWithValue("@id", voterId);
                cmd.Parameters.AddWithValue("@vote", vote);
                cmd.ExecuteNonQuery();
            }
            catch (DbException)
            {
                cmd.CommandText =
                    "UPDATE votes SET vote = @vote WHERE id = @id";
                cmd.ExecuteNonQuery();
            }
        }
    }
}
